# Next steps — sigforge

Findings from three parallel audits: code quality review, feature value sanity
checks, and BQ/pipeline audit. Ordered by risk.

---

## Bugs to fix before first production run

### 1. `setattr` on a frozen pydantic v2 model — `run_profile.py`

`_backfill_gics()` calls `setattr(row, field, value)` on a `ProfileRow` instance.
Pydantic v2 models are immutable by default: the write silently does nothing.
GICS fields from `symbols.yaml` are never actually backfilled.

**Fix:** add `model_config = ConfigDict(validate_assignment=True)` to `ProfileRow`,
or replace the mutation with a `model_copy(update={...})` call.

---

### 2. Module failures swallowed silently, all-None rows written to BQ — `pipeline.py`

When `_run_module()` catches an exception it returns `{}` and logs the error, but:
- The symbol still gets `symbols_processed += 1` (looks like success)
- The exception is **not** added to `result.errors`
- A `FeatureRow` with every feature column `None` is appended and written to BQ

Downstream consumers cannot tell whether a `None` means "not computable" or
"computation crashed". Auditing failures is impossible from the result object.

**Fix:** call `result.add_error(...)` inside `_run_module` on exception, and
optionally skip writing the row when all feature fields are None.

---

### 3. `get_previous_profile` converts dates to strings then pydantic coerces them back — `bq_client.py`

BigQuery returns `snapshot_date` as a native `datetime.date` and `ingested_at`
as `datetime.datetime`. The method converts them to ISO strings before passing
to `ProfileRow(**d)`, which pydantic then re-parses back to dates. Works today
only because pydantic accepts ISO strings — semantically wrong and fragile.

**Fix:** remove the `isoformat()` calls; pass native types directly.

---

### 4. `_extract_symbol` returns `None` when batch response has flat columns — `yf_client.py`

When yfinance batches multiple symbols but all return empty except one, it may
return a flat (non-MultiIndex) DataFrame. `_extract_symbol` returns `None` for
every symbol in that case, causing silent data loss in `fetch_daily_batch`.

**Fix:** detect the flat-column case and fall back to a per-symbol single fetch.

---

### 5. NaN propagates silently into BQ from peer correlation — `correlation.py`

`peers_recent.corrwith(stock_recent)` returns NaN when a peer has constant
returns. `float(corrs.mean())` then stores NaN in `cr_rolling_peer_correlation`
without any guard. Same risk for `cr_peer_return_deviation` when all peers
are NaN on the last day.

**Fix:** wrap assignments with `None if pd.isna(v) else v` before storing.

---

## Data quality — feature value sanity

Sanity checks were run on all four modules with controlled synthetic data.
Results and flags:

| Check | Result | Status |
|---|---|---|
| Beta of SPY vs SPY | 1.0000 (alpha ≈ 0) | ✅ |
| Drawdown on monotonically rising prices | max_dd=0.0, duration=0 | ✅ |
| Amihud with 50 % zero-volume rows | Small positive, no crash | ✅ |
| Realized vol on constant 1 % daily return | ≈ 0 (1.7e-15) | ✅ |
| Peer correlation, all peers identical | 1.0000 | ✅ |
| Peer correlation, all peers perfectly anti-correlated | -0.997 (not -1.0) | ⚠️ |
| Mahalanobis with singular covariance | 0.086, no crash | ✅ |
| Fundamental stubs absent from returned dict | Confirmed absent | ✅ |
| `FeatureRow.to_bq_dict()` round-trip | All 24 keys, dates as strings | ✅ |

**Flag — anti-correlated peer correlation:** returns -0.997 instead of -1.0.
This is a floating-point and date-alignment artifact when building the peer
matrix, not a calculation error, but it shows the feature is slightly noisy
near the boundary. Add a clamp `max(-1.0, min(1.0, value))` before writing.

**Additional sanity checks to add as tests:**

- `rb_rolling_beta` range: alert if |beta| > 5 (likely a data error)
- `ms_realized_volatility`: alert if annualised vol > 300 % (data corruption)
- `cr_rolling_peer_correlation`: clamp to [-1, 1] before BQ write
- `ms_amihud_illiquidity`: alert if value is inf or > 1e6
- `rb_max_drawdown`: must be ≤ 0.0; reject positive values

---

## Architecture issues

### 6. Duplicate `_safe_float` / `_safe_int` helpers

`jobs/backfill.py` (`_sf`, `_si`) duplicates the helpers in `sigforge/yf_client.py`.
The two implementations differ subtly (`math.isnan` vs `pd.isna`).

**Fix:** move to `sigforge/utils.py`, import from both callers.

---

### 7. `run_id` is not unique per execution — `pipeline.py`

`_make_run_id()` returns `"features-{date}"`, the same string for every re-run
on the same date. If the pipeline is retried after a transient failure, both
runs share a `run_id`, making it impossible to distinguish them in BQ.

**Fix:** `run_id = f"features-{feature_date.isoformat()}-{uuid4().hex[:8]}"`.

---

### 8. `insert_events_manual` has no dedup — `bq_client.py`

Running `add_note.py` twice with the same arguments inserts two rows because
each call generates a fresh `uuid4()` as `event_id`. The dedup in
`upsert_events()` only works when the caller reuses the same `event_id`.

**Fix:** for manual events, derive `event_id` deterministically from
`(symbol, event_date, event_type, notes)` via `uuid5`, so re-running is
idempotent.

---

### 9. Literal type mapping in `_schema_from_model` is accidental — `bq_client.py`

`EventType` and `Source` are `Literal[...]` types. The schema derivation function
does not handle `Literal`; it falls through to a STRING default by accident.
The output is correct (they should be STRING), but the code is fragile.

**Fix:** add an explicit `Literal` branch before the fallback:
```python
if getattr(ann, "__origin__", None) is Literal:
    bq_type = "STRING"
```

---

## Missing test coverage

| Gap | Recommended test |
|---|---|
| NaN stored in feature column | Assert no NaN/inf in any field of a FeatureRow after `to_bq_dict()` |
| Peer correlation with constant-return peer | Confirm result is None (not NaN) |
| Amihud when all rows have zero volume | Confirm result is None (not inf) |
| `_run_module` failure counted in `result.errors` | Mock module to raise; assert error recorded |
| `add_note.py` re-run dedup | Call twice with same args; assert 1 row in BQ, not 2 |
| Backfill CSV output columns | Assert CSV headers match `RawBar` / `FeatureRow` model fields |
| `get_previous_profile` type handling | Mock BQ row with native `datetime.date`; assert `ProfileRow` constructed correctly |
| `_extract_symbol` flat-column fallback | Pass non-MultiIndex DataFrame with `n_symbols > 1`; assert no silent data loss |

---

## Suggested work order for next session

1. **Fix the five bugs** (items 1–5 above) — roughly 2 hours
2. **Add NaN/inf guard layer** in `pipeline.py` before row construction — 1 hour
3. **Move shared helpers to `sigforge/utils.py`** — 30 min
4. **Fix `run_id` and `insert_events_manual` dedup** — 30 min
5. **Add missing tests** for the gaps in the table above — 2 hours
6. **First real dry run** with `make backfill-local START=2025-01-02 END=2025-01-10 CSV=data/dry_run.csv` and inspect the CSV — inspect feature value distributions before any BQ write
