# sigforge

Feature engineering pipeline for individual stocks. Fetches raw data via yfinance,
computes ML-ready behavioral features, detects structural events, stores in BigQuery.

## Stack
- Python 3.12, pydantic-settings, pydantic v2, numpy, pandas
- yfinance for market data
- Google BigQuery (dataset: sigforge_prod / sigforge_stage)
- Cloud Run Jobs + Cloud Scheduler for production
- ruff for linting, pytest for tests

## Key conventions
- No hardcoded field lists — derive CSV columns and BQ schema from pydantic model fields
- Feature columns are prefixed by module: rb_ (return-based), ms_ (microstructure),
  cr_ (correlation/peer), fu_ (fundamental)
- Partial failure policy: one symbol or module failing must never abort the full run —
  log error, fill affected columns with None, continue
- All tests mock yfinance and BQ — zero network calls in tests
- Upserts are idempotent: re-running the same date must not duplicate rows

## Project layout
- `sigforge/` — Python package (settings, models, yf_client, bq_client, features/)
- `config/symbols.yaml` — stock universe with full GICS classification
- `jobs/` — run_daily.py, run_profile.py, backfill.py, add_note.py
- `tests/test_features/` — one file per feature module
- `deploy/Dockerfile` — python:3.12-slim, non-root user

## BQ tables (sigforge_{env})
- `ticker_daily`    — upsert on (symbol, trade_date), daily OHLCV + avg_volume_30d
- `ticker_profile`  — upsert on (symbol, snapshot_date), ~6-week profile snapshots
- `ticker_features` — upsert on (symbol, feature_date), wide table, all feature columns
- `ticker_events`   — upsert on event_id

## Feature modules (sigforge/features/)
- `return_based.py`   — rb_* prefix, 63-day window, OLS vs SPY, drawdown, autocorr
- `microstructure.py` — ms_* prefix, 21-day Amihud, realized vol, volume ratio, HL range
- `correlation.py`    — cr_* prefix, 63-day peer correlation, Mahalanobis, lead-lag
- `fundamental.py`    — fu_* prefix, PE ratio, short interest from ticker.info
- `pipeline.py`       — orchestrator: runs all modules per symbol, returns List[FeatureRow]

## Stubs (do not implement, do not remove)
- `return_based.py`: fama_french_residual — needs pandas-datareader / FF data library
- `microstructure.py`: bid_ask_spread, implied_vol_spread, intraday_vol_pattern
- `fundamental.py`: earnings_revision_momentum, analyst_estimate_dispersion

## Deferred decisions
- Fama-French factor data source (pandas-datareader vs other)
- Microstructure data beyond yfinance (bid-ask, order book, options)
- Fundamental data vendor for analyst estimates (Refinitiv, Bloomberg)

## Environment
- `.env` file: GCP_PROJECT, DW_ENV, ANTHROPIC_API_KEY
- `config/symbols.yaml`: stocks with ticker, name, and full GICS classification
- `sigforge/settings.py`: pydantic-settings, validates on startup, exposes load_tickers()

## Makefile targets
- `install`, `lint`, `test`
- `run-daily`, `run-profile`, `add-note`
- `backfill-local START=YYYY-MM-DD END=YYYY-MM-DD [CSV=path]`
- `gcp-setup`, `bq-init`
- `docker-build`, `docker-push`
- `deploy-daily`, `deploy-profile`, `deploy-all`
- `scheduler-daily`, `scheduler-profile`

## Market proxy
- SPY is fetched alongside every symbol for return-based feature computation
- Called explicitly in run_daily.py and backfill.py via get_history("SPY")
