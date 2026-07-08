# teamfish — feature definitions

Reference for every column in `ticker_features`. The authoritative column list is
`FeatureRow` in [`teamfish/models.py`](teamfish/models.py); definitions below come from
the `compute()` functions in [`teamfish/features/`](teamfish/features/). If this doc
and the code disagree, the code wins — update this doc.

Known correctness caveats (in-sample residual beta, volume-ratio denominator,
fundamentals timestamping) are tracked in
[README-pipeline-review.md](README-pipeline-review.md) and noted inline below.

## Row key

| Column | Meaning |
|---|---|
| `symbol` | Ticker. |
| `feature_date` | Date the features describe. Upsert key with `symbol`. |
| `run_id` | `features-<date>-<hex8>`, unique per `pipeline.run` call (traceability; excluded from CSV output). |

`ingested_at` (UTC timestamp) is set on every row; excluded from CSV output.

## Point-in-time guarantee

`pipeline.run` truncates all bar histories (`raw_bars`, `spy_bars`) to
`index <= feature_date` before any module runs (`_truncate` in
[`teamfish/features/pipeline.py`](teamfish/features/pipeline.py)), so no price-based
feature can see bars after `feature_date` regardless of what the caller pre-fetched.
Regression-tested by
`tests/test_features/test_pipeline.py::test_future_bars_do_not_affect_features`.
Background: this guard was added 2026-07-08 after a backfill look-ahead bug — see
[README-pipeline-review.md](README-pipeline-review.md) §4. The guarantee does **not**
extend to `fu_*` (fundamentals are as-of-fetch-date, see below) or to sector
membership used by `cr_*`.

## Failure / None semantics

Two layers, per the partial-failure policy in `CLAUDE.md`:

1. **Module granularity** — each module runs inside `pipeline._run_module`
   (try/except). An exception nulls that module's columns only; the run continues.
2. **Feature granularity** — inside a module, any feature whose preconditions fail
   (too few observations, missing columns, zero denominators) is omitted from the
   result dict and defaults to `None` on `FeatureRow`. `pipeline._sanitize` converts
   any surviving `inf`/`nan` to `None`.

Notation below: `r_t` = daily simple return (`Close.pct_change()` on the adjusted
close), `m_t` = SPY return, `V_t` = volume, `C/H/L` = adjusted close / high / low.

---

## Return-based — `rb_*` ([`return_based.py`](teamfish/features/return_based.py))

Window `WINDOW = 63` trading days, `MIN_OBS = 20`. Returns are computed from the
adjusted `Close`, aligned with SPY on common dates (inner join), last 63 observations.
If fewer than 20 aligned observations, **all** `rb_*` are `None`. Data source:
yfinance adjusted daily bars for the symbol + SPY (market proxy).

OLS is `np.polyfit(m, s, deg=1)`: $s_t = \alpha + \beta\, m_t$.

| Column | Definition | Window | None when |
|---|---|---|---|
| `rb_rolling_alpha` | OLS intercept $\alpha$ | 63d | < 20 aligned obs |
| `rb_rolling_beta` | OLS slope $\beta$ | 63d | < 20 aligned obs |
| `rb_residual_return` | $s_t - \beta\, m_t$ for the most recent day. Beta fit includes day *t* (in-sample by one day — review doc §5.6) | 63d fit, 1d value | < 20 aligned obs |
| `rb_return_autocorr` | Lag-1 autocorrelation of the 63d return series (`pd.Series.autocorr(lag=1)`) | 63d | autocorr computation fails |
| `rb_rolling_skewness` | `pd.Series.skew()` of 63d returns (Fisher) | 63d | < 20 aligned obs |
| `rb_rolling_kurtosis` | `pd.Series.kurtosis()` of 63d returns (Fisher, **excess** — normal = 0) | 63d | < 20 aligned obs |
| `rb_max_drawdown` | $\min_t \frac{P_t - \max_{s \le t} P_s}{\max_{s \le t} P_s}$ on cumulative returns from the last 64 closes (63 returns). ≤ 0 | 63d | < 2 closes |
| `rb_drawdown_duration` | Longest consecutive run of days with cumulative return below its running max (int, days) | 63d | < 2 closes |

## Microstructure — `ms_*` ([`microstructure.py`](teamfish/features/microstructure.py))

`AMIHUD_WINDOW = 21`, `REALIZED_VOL_WINDOW = 21`, `VOLUME_AVG_WINDOW = 30`,
`MIN_OBS = 5`. All `ms_*` are `None` if bars lack any of Open/High/Low/Close/Volume or
have < 5 rows. Data source: the symbol's own yfinance adjusted daily bars only.

| Column | Definition | Window | None when |
|---|---|---|---|
| `ms_amihud_illiquidity` | $\text{mean}\left(\frac{\|r_t\|}{C_t V_t}\right)$ over the last 21 bars; zero-dollar-volume days dropped | 21d | < 5 bars in window, or all dollar volumes zero |
| `ms_volume_ratio` | $V_{\text{today}} / \text{mean}(V, 30\text{d})$. **Today is inside the 30d average** — spikes damped ~1/30 (review doc §5.5) | 30d | 30d avg volume is 0/NaN |
| `ms_realized_volatility` | $\text{std}(\log \frac{C_t}{C_{t-1}}, 21\text{d}) \times \sqrt{252}$ (annualised) | 21d | < 5 log returns in window |
| `ms_high_low_range` | $(H - L) / C$ for the last bar | 1d | H/L missing or C = 0 |

## Correlation / peer — `cr_*` ([`correlation.py`](teamfish/features/correlation.py))

`CORR_WINDOW = 63`, `HIST_WINDOW = 252`, `MAHAL_OBS = 21`, `MIN_PEERS = 2`,
`MIN_OBS = 20`. Peers = other universe symbols in the same GICS sector
(`config/symbols.yaml` via `get_sector_map()`; membership is current-state, not
point-in-time). All peer return series are aligned by joint `dropna()` — one gappy
peer shrinks the sample for the whole sector (review doc §5.4). All `cr_*` are `None`
if the stock has < 20 returns, fewer than 2 peers each with ≥ 20 returns, or < 20
jointly aligned observations. Data source: yfinance adjusted daily bars for the
symbol + sector peers.

Let $c_t$ = equal-weight mean of peer returns (sector centroid) over the 63d window.

| Column | Definition | Window | None when |
|---|---|---|---|
| `cr_rolling_peer_correlation` | Mean over peers of Pearson $\text{corr}(s, p_i)$, clipped to $[-1, 1]$ | 63d | all pairwise corrs NaN |
| `cr_peer_return_deviation` | $s_{\text{today}} - \text{median}_i(p_{i,\text{today}})$ | 1d | peer median NaN |
| `cr_correlation_breakdown_score` | Mahalanobis distance $\sqrt{v^\top \Sigma^{+} v}$, where $v = \text{mean}(p, 21\text{d}) - \text{mean}(p, 252\text{d})$ per peer and $\Sigma$ = 252d peer covariance (Moore–Penrose `pinv`). ≥ 0; large ⇒ recent peer co-movement atypical | 21d obs vs 252d dist | < 42 aligned history rows, or linalg failure |
| `cr_lead_lag_score` | $\text{corr}(s_t, c_{t+1}) - \text{corr}(s_t, c_{t-1})$. Positive ⇒ stock leads its sector; negative ⇒ lags | 63d | < 5 valid overlapping pairs at either lag |

## Fundamental — `fu_*` ([`fundamental.py`](teamfish/features/fundamental.py))

Straight from `yf.Ticker(symbol).info` via `safe_float` — no window. Each field is
independently `None` when the info key is missing or non-numeric. Unlike the other
modules, `compute()` always returns every key (with `None` values where unavailable).

**Timestamping caveat:** `ticker.info` is as-of-fetch-date, not point-in-time. In a
backfill, every historical `feature_date` gets the *run date's* fundamentals — see
[README-pipeline-review.md](README-pipeline-review.md) §5.2.

| Column | `ticker.info` source | Definition |
|---|---|---|
| `fu_pe_ratio` | `trailingPE`, fallback `forwardPE` | Price/earnings |
| `fu_short_interest_ratio` | `shortRatio` | Days-to-cover |
| `fu_short_pct_float` | `shortPercentOfFloat` | Short interest as fraction of float |
| `fu_float_turnover` | `floatShares` + last bar's `Volume` | $V_{\text{today}} / \text{floatShares}$; `None` if float ≤ 0 or no bars |
| `fu_gross_margin` | `grossMargins` | Gross margin |
| `fu_operating_margin` | `operatingMargins` | Operating margin |
| `fu_profit_margin` | `profitMargins` | Net profit margin |
| `fu_return_on_equity` | `returnOnEquity` | ROE |
| `fu_return_on_assets` | `returnOnAssets` | ROA |
| `fu_debt_to_equity` | `debtToEquity` | D/E |
| `fu_revenue_growth` | `revenueGrowth` | Quarterly YoY revenue growth |
| `fu_earnings_growth` | `earningsGrowth` | Quarterly YoY earnings growth |

## Stubs — deferred, do not implement, do not remove

Each stub is blocked on a data vendor decision (CLAUDE.md "Deferred decisions"). The
two `fu_` stubs exist as always-`None` columns on `FeatureRow`; the `rb_`/`ms_` stubs
are comment-only in their modules and are **not** yet columns.

| Stub | Module | Blocking dependency | On `FeatureRow`? |
|---|---|---|---|
| `rb_fama_french_residual` | `return_based.py` | Fama–French factor data source (pandas-datareader vs other) | No |
| `ms_bid_ask_spread` | `microstructure.py` | Level 1 quote data (beyond yfinance) | No |
| `ms_implied_vol_spread` | `microstructure.py` | Options chain data (beyond yfinance) | No |
| `ms_intraday_vol_pattern` | `microstructure.py` | Intraday OHLCV (beyond yfinance) | No |
| `fu_earnings_revision_momentum` | `fundamental.py` | Consensus estimate history (Refinitiv/Bloomberg) | Yes — always `None` |
| `fu_analyst_estimate_dispersion` | `fundamental.py` | Analyst estimate distribution (Refinitiv/Bloomberg) | Yes — always `None` |
