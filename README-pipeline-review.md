# teamfish — pipeline review (2026-07-08)

Code review of the feature pipeline: what it does, one critical bug found and fixed
same-day, and a prioritized backlog of remaining issues. Feature-by-feature definitions
live in [README-features.md](README-features.md).

Files reviewed: [`teamfish/yf_client.py`](teamfish/yf_client.py),
[`teamfish/features/pipeline.py`](teamfish/features/pipeline.py),
[`teamfish/features/*.py`](teamfish/features/),
[`jobs/backfill.py`](jobs/backfill.py), [`jobs/run_daily.py`](jobs/run_daily.py).

---

## 1. What the pipeline does

`jobs/backfill.py` and `jobs/run_daily.py` pre-fetch adjusted daily history per symbol
plus SPY via `yf_client.get_history`, and `ticker.info` per symbol via `get_info`.
`features/pipeline.run` then runs four modules per symbol:

| Module | Prefix | Core computations |
|---|---|---|
| `return_based.py` | `rb_` | 63d OLS vs SPY (alpha/beta/residual), max drawdown + duration, skew/kurtosis, lag-1 autocorr |
| `microstructure.py` | `ms_` | 21d Amihud illiquidity, 21d realized vol, volume ratio vs 30d avg, high-low range |
| `correlation.py` | `cr_` | 63d mean peer correlation, Mahalanobis breakdown score, lead-lag vs sector centroid, peer return deviation |
| `fundamental.py` | `fu_` | Ratios straight from `ticker.info` (PE, short interest, margins, growth) |

Each module call is wrapped in `pipeline._run_module` (try/except): a failing module
logs an error, its columns stay `None`, and the run continues — the partial-failure
policy from `CLAUDE.md` enforced at module granularity. Results are sanitized
(`inf`/`nan` → `None` in `_sanitize`) into a `FeatureRow` keyed by
`(symbol, feature_date)` with a per-run `run_id`.

## 2. Adjusted close — yes, with a point-in-time caveat

`get_history` and `fetch_daily_batch` both fetch with `auto_adjust=True`: the `Close`
column is split-and-dividend adjusted, stored as `RawBar.adj_close`, and all return
features are computed on total-return prices. Correct choice for the features.

**Caveat:** adjusted prices are not point-in-time stable. A future dividend
retroactively rescales every past adjusted close. Daily incremental upserts into
`ticker_daily` therefore accumulate rows adjusted as-of *different ingestion dates* —
after any dividend, the stored series no longer chains into a clean return series. And
because `auto_adjust=True` discards the raw close, the information is unrecoverable
from what we store.

**Recommended fix (open decision, not implemented):** fetch with `auto_adjust=False`,
store raw close plus `Adj Close` (or an adjustment factor), and adjust at read time —
or explicitly accept the drift and re-backfill after dividend events. Either way,
**decide before the first production BQ load** (see
[README-next-steps.md](README-next-steps.md)); changing the `ticker_daily` schema
afterward means a table drop and full re-backfill.

## 3. Why features are split across four modules (keep as-is)

The split is load-bearing, not cosmetic:

- **Different inputs per module.** `return_based` needs SPY bars, `correlation` needs
  the sector peer map, `fundamental` needs `ticker.info`, `microstructure` needs only
  the symbol's own bars. Each `compute()` signature declares exactly what it depends on.
- **Partial-failure granularity.** `pipeline._run_module` wraps each module separately —
  a `ticker.info` outage kills only `fu_*`, never `rb_*`/`ms_*`/`cr_*`.
- **1:1 mapping everywhere.** Column prefixes (`rb_`/`ms_`/`cr_`/`fu_`), test files
  (`tests/test_features/test_<module>.py`), and stubs (each blocked on a different
  deferred data vendor — see CLAUDE.md "Deferred decisions") all map one-to-one to files.
- **Single composition point.** `pipeline.py` is the only place that knows about all
  modules; adding a module touches one dispatch list.

## 4. Critical bug — backfill look-ahead bias (FIXED 2026-07-08)

**The bug.** `backfill.py` pre-fetched one history per symbol ending at `--end`, then
called `pipeline.run` once per trade date **with the same untruncated frames**. Every
module reads the *end* of its input (`.tail(WINDOW)`, `.iloc[-1]`), so every backfill
date received features computed from the window ending at `--end`. Verified
empirically before the fix: in the pre-fix dry-run CSV (`April-June-2026_features.csv`),
AAPL had exactly **1 unique feature vector across ~70 dates** — all rows byte-identical.

**The fix** (two parts, both in the code now):

1. `pipeline.run` truncates `raw_bars` and `spy_bars` to `index <= feature_date`
   before dispatching to any module (`_truncate` helper in
   [`teamfish/features/pipeline.py`](teamfish/features/pipeline.py)). Point-in-time
   correctness is now **structural** — enforced inside the pipeline — rather than
   dependent on every caller passing pre-truncated frames.
2. [`jobs/backfill.py`](jobs/backfill.py) extends the pre-fetch lookback by the
   trading-day span of the backfill (`lookback = settings.history_days + span`), so
   after truncation the *earliest* date in the range still has a full
   `history_days` window behind it.

**Regression test:**
`tests/test_features/test_pipeline.py::test_future_bars_do_not_affect_features` —
features for an early date computed from a history extending past it must equal
features computed from a history ending on that date. Suite: **99 passed**
(`tests/test_ohlcv_daily.py` excluded locally — legacy `driftwatch/` test that requires
GCP ADC credentials at collection time; environmental, unrelated to this change).

**Post-fix verification** (dry run rerun, 2026-04-01 → 2026-07-07, 60 symbols):

- 3960 daily rows, 4200 feature rows, 0 errors.
- AAPL: **66 unique feature vectors across 70 weekday rows.** The 4 duplicates are
  exactly the US market holidays in range — Good Friday (2026-04-03), Memorial Day
  (2026-05-25), Juneteenth (2026-06-19), Independence Day observed (2026-07-03) —
  where `trading_days()` yields a weekday with no bar, so the truncated window equals
  the prior day's.

**Holiday duplicates — also fixed (2026-07-08):** the initial fix still *emitted*
feature rows for market holidays, duplicating the prior day's vector (the 4200 vs 3960
row counts above). `pipeline.run` now skips any symbol with no bar on `feature_date`
itself — market holiday or symbol-specific gap — counted in
`PipelineResult.symbols_skipped`, not treated as an error. Feature rows now match daily
rows 1:1 (3960/3960), and AAPL shows 66 unique vectors across 66 rows. `run_daily.py`
is unaffected: on a holiday it already exits before feature computation (no OHLCV rows).
Regression test:
`tests/test_features/test_pipeline.py::test_no_bar_on_feature_date_skips_symbol`.

## 5. Remaining known issues / backlog (not yet fixed)

In priority order:

1. **Raw + adjusted close schema decision** (section 2). Decide before the first
   production BQ load — the only item here that is expensive to reverse.
2. **Fundamentals can't be backfilled honestly.** `info_dict` is run-date
   `ticker.info` stamped onto every historical `feature_date`: `fu_pe_ratio` on a
   backfill date is really the *run date's* PE. yfinance has no point-in-time
   fundamentals. Options: null out `fu_*` in backfill runs, or document the columns as
   as-of-run-date. Same issue in principle for `cr_*` sector membership
   (`get_sector_map()` is current-state), but GICS churn is slow enough to ignore.
3. **`yf_client._history_cache` keyed by symbol only**, ignoring `end_date` and
   `lookback_days` — a second `get_history` call in the same process with different
   args silently returns the first call's window. Key by
   `(symbol, end_date, lookback_days)`.
4. **`correlation.py` joint `dropna()` over the whole sector.** One peer with a
   short/gappy history shrinks the aligned sample for the entire sector, potentially
   below `MIN_OBS`. Drop peers with insufficient overlap before aligning, or compute
   pairwise.
5. ~~**`ms_volume_ratio` includes today in its own denominator**~~ **FIXED
   (2026-07-08)**: the 30-day average now excludes today
   (`test_volume_ratio_excludes_today_from_average`). `avg_volume_30d` in
   `backfill.py`/`fetch_daily_batch` intentionally keeps the inclusive trailing
   average — it is a stored stat, not a spike detector.
6. ~~**`rb_residual_return` beta is in-sample by one day**~~ **FIXED (2026-07-08)**:
   α/β for the residual are now fit on *t−63 … t−1* and applied to day *t*
   (`test_residual_return_uses_out_of_sample_fit`). Semantics also changed from
   $s_t - \beta m_t$ to the full out-of-sample prediction error
   $s_t - (\alpha' + \beta' m_t)$ — the residual is now centred (the stock's own
   drift is subtracted). `rb_rolling_beta`/`rb_rolling_alpha` remain full-window
   descriptive fits.
7. **Cheap candidate features** from data already fetched, no new vendor needed:
   21/63/126d momentum, distance from 52-week high, Garman–Klass / Parkinson OHLC
   volatility, volume trend, dollar volume, days since last split/dividend.
