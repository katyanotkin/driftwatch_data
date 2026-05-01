# sigforge — next steps

Current state: pipeline is feature-complete, 101 tests pass, dry-run CSV validated.
Schema enrichments (adjusted close, corporate actions, 13 new profile fields, 10 new
fundamental features) merged. Ready for first production BQ write and Cloud Run deployment.

---

## 1. First production data load

### 1a. Initialise BQ tables

```bash
make bq-init ENV=prod
```

> If tables already exist with the old schema (no `adj_close`, `dividends`, etc.) you
> must drop them first — BigQuery does not add columns automatically:
>
> ```bash
> bq rm -f -t driftwatch2:sigforge_prod.ticker_daily
> bq rm -f -t driftwatch2:sigforge_prod.ticker_profile
> bq rm -f -t driftwatch2:sigforge_prod.ticker_features
> bq rm -f -t driftwatch2:sigforge_prod.ticker_events
> make bq-init ENV=prod
> ```

### 1b. Backfill historical OHLCV + features

Start with one month to validate end-to-end before going deeper:

```bash
make backfill-local START=2025-01-02 END=2025-01-31 CSV=data/jan25.csv
```

Inspect `data/jan25_daily.csv` and `data/jan25_features.csv`, then write to BQ:

```bash
PYTHONPATH=. python jobs/backfill.py --start 2025-01-02 --end 2025-01-31
```

Full history (recommended starting point: 1 year):

```bash
PYTHONPATH=. python jobs/backfill.py --start 2024-01-02 --end 2025-04-30
```

### 1c. Profile snapshot

```bash
make run-profile ENV=prod
```

---

## 2. Cloud Run deployment

```bash
make docker-build
make docker-push
make deploy-daily ENV=prod
make deploy-profile ENV=prod
```

Or all at once:

```bash
make deploy-all ENV=prod
```

### Set up Cloud Scheduler triggers

```bash
make scheduler-daily ENV=prod    # weekdays 22:00 UTC
make scheduler-profile ENV=prod  # Sundays 23:00 UTC (≈ 6-week cadence)
```

---

## 3. Validate first live run

After the scheduler fires (or trigger manually):

```bash
gcloud run jobs execute sigforge-daily-prod --region=us-central1 --project=driftwatch2 --wait
```

Check BQ:

```sql
-- Spot-check features for the most recent date
SELECT symbol, feature_date, rb_rolling_beta, ms_realized_volatility,
       fu_float_turnover, fu_short_pct_float
FROM `driftwatch2.sigforge_prod.ticker_features`
WHERE feature_date = (SELECT MAX(feature_date) FROM `driftwatch2.sigforge_prod.ticker_features`)
ORDER BY symbol
LIMIT 20;
```

Sanity thresholds to check manually:
| Feature | Concern if... |
|---|---|
| `rb_rolling_beta` | \|value\| > 5 |
| `ms_realized_volatility` | > 3.0 (300 % annualised) |
| `ms_amihud_illiquidity` | > 1e6 or NULL for liquid names |
| `rb_max_drawdown` | > 0.0 (must be ≤ 0) |
| `fu_float_turnover` | > 1.0 (full float traded in one day) |
| `cr_rolling_peer_correlation` | outside [-1, 1] |

---

## 4. Near-term feature work

### 4a. Fama-French residual (return_based.py stub)

`fama_french_residual` is stubbed out. Ken French publishes factor data for free;
`pandas-datareader` can fetch it:

```python
import pandas_datareader.data as web
ff = web.DataReader("F-F_Research_Data_Factors_daily", "famafrench", start, end)[0]
```

Regress `(stock_return - rf)` on `Mkt-RF`, `SMB`, `HML` to get the alpha residual.
This is the highest-signal addition remaining in the return-based module.

### 4b. Corporate-action event auto-detection

`RawBar` now stores `dividends` and `split_ratio` on ex-dates.
Wire up automatic event creation in `run_daily.py`:
- if `dividends > 0` → insert `EventRow(event_type="dividend", ...)`
- if `split_ratio` is not None → insert `EventRow(event_type="split", ...)`

This makes the `ticker_events` table self-populating for the most common corporate actions.

### 4c. float_turnover anomaly events

`fu_float_turnover > 0.05` (5 % of float traded in a day) is a reliable precursor
to large moves. Add a post-pipeline check in `run_daily.py` that creates an
`EventRow(event_type="manager_note", details="float_turnover spike: ...")` when
the threshold is crossed.

---

## 5. Deferred / paid-data stubs

These stubs exist in `fundamental.py` and `FeatureRow` — do not implement until
a data vendor is selected:

| Stub | Needs |
|---|---|
| `fu_earnings_revision_momentum` | Consensus estimate history (Refinitiv / Bloomberg) |
| `fu_analyst_estimate_dispersion` | Analyst estimate distribution (same vendors) |

Microstructure stubs in `microstructure.py`:
| Stub | Needs |
|---|---|
| `bid_ask_spread` | Intraday tick data or options market data |
| `implied_vol_spread` | Options chain (yfinance provides this — feasible) |
| `intraday_vol_pattern` | 1-min intraday bars |

`implied_vol_spread` via yfinance options is the lowest-hanging fruit here.

---

## 6. Monitoring

- Add a `PipelineResult`-based alert: if `result.errors` is non-empty after `run_daily`,
  send a notification (Cloud Monitoring log-based alert or simple email via SendGrid).
- Track `symbols_processed` count over time — a sudden drop signals a yfinance outage
  or symbol delistings.
