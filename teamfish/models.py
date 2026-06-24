from __future__ import annotations

import datetime
from typing import Any, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

EventType = Literal[
    # Corporate actions
    "split",
    "reverse_split",
    "dividend",
    "special_dividend",
    # Structural / index
    "gics_reclassification",
    "index_addition",
    "index_removal",
    # Fundamental signals
    "earnings_surprise",
    "guidance_change",
    "analyst_rating_change",
    # Regulatory / news (Claude-detected)
    "fda_approval",
    "fda_rejection",
    "legal_action",
    "merger_acquisition",
    # Manual
    "manager_note",
]

Source = Literal["claude_auto", "manual"]


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class RawBar(BaseModel):
    symbol: str
    trade_date: datetime.date
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    # Split-and-dividend adjusted close (yfinance auto_adjust=True)
    adj_close: Optional[float] = None
    volume: Optional[int] = None
    avg_volume_30d: Optional[float] = None
    # Corporate actions — non-None only on the event date
    dividends: Optional[float] = None   # raw dividend per share on ex-date
    split_ratio: Optional[float] = None  # e.g. 2.0 for a 2-for-1 split
    data_source: str = "yfinance"
    ingested_at: datetime.datetime = Field(default_factory=utc_now)

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["trade_date"] = data["trade_date"].isoformat()
        data["ingested_at"] = data["ingested_at"].isoformat()
        return data

    def to_csv_dict(self) -> dict[str, Any]:
        data = self.model_dump(exclude={"ingested_at", "data_source"})
        data["trade_date"] = data["trade_date"].isoformat()
        return data


class ProfileRow(BaseModel):
    symbol: str
    snapshot_date: datetime.date
    name: Optional[str] = None
    exchange: Optional[str] = None
    gics_sector: Optional[str] = None
    gics_industry_group: Optional[str] = None
    gics_industry: Optional[str] = None
    gics_sub_industry: Optional[str] = None
    market_cap: Optional[float] = None
    enterprise_value: Optional[float] = None
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    payout_ratio: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    beta: Optional[float] = None
    shares_outstanding: Optional[float] = None
    float_shares: Optional[float] = None          # tradeable float (excl. locked-up shares)
    shares_short: Optional[float] = None
    short_ratio: Optional[float] = None           # days-to-cover
    short_pct_float: Optional[float] = None       # short interest as % of float
    institutional_hold_pct: Optional[float] = None
    insider_hold_pct: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    profit_margin: Optional[float] = None
    return_on_equity: Optional[float] = None
    return_on_assets: Optional[float] = None
    debt_to_equity: Optional[float] = None
    revenue_growth: Optional[float] = None        # quarterly YoY
    earnings_growth: Optional[float] = None       # quarterly YoY
    free_cash_flow: Optional[float] = None        # trailing 12-month
    analyst_target_price: Optional[float] = None
    analyst_recommendation: Optional[str] = None
    data_source: str = "yfinance"
    ingested_at: datetime.datetime = Field(default_factory=utc_now)

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["snapshot_date"] = data["snapshot_date"].isoformat()
        data["ingested_at"] = data["ingested_at"].isoformat()
        return data


class FeatureRow(BaseModel):
    symbol: str
    feature_date: datetime.date
    run_id: str
    ingested_at: datetime.datetime = Field(default_factory=utc_now)

    # Return-based (rb_)
    rb_rolling_alpha: Optional[float] = None
    rb_rolling_beta: Optional[float] = None
    rb_residual_return: Optional[float] = None
    rb_return_autocorr: Optional[float] = None
    rb_rolling_skewness: Optional[float] = None
    rb_rolling_kurtosis: Optional[float] = None
    rb_max_drawdown: Optional[float] = None
    rb_drawdown_duration: Optional[int] = None

    # Microstructure (ms_)
    ms_amihud_illiquidity: Optional[float] = None
    ms_volume_ratio: Optional[float] = None
    ms_realized_volatility: Optional[float] = None
    ms_high_low_range: Optional[float] = None

    # Correlation/peer (cr_)
    cr_rolling_peer_correlation: Optional[float] = None
    cr_peer_return_deviation: Optional[float] = None
    cr_correlation_breakdown_score: Optional[float] = None
    cr_lead_lag_score: Optional[float] = None

    # Fundamental (fu_)
    fu_pe_ratio: Optional[float] = None
    fu_short_interest_ratio: Optional[float] = None   # days-to-cover
    fu_short_pct_float: Optional[float] = None        # short interest as % of float
    fu_float_turnover: Optional[float] = None         # daily volume / float shares
    fu_gross_margin: Optional[float] = None
    fu_operating_margin: Optional[float] = None
    fu_profit_margin: Optional[float] = None
    fu_return_on_equity: Optional[float] = None
    fu_return_on_assets: Optional[float] = None
    fu_debt_to_equity: Optional[float] = None
    fu_revenue_growth: Optional[float] = None
    fu_earnings_growth: Optional[float] = None
    fu_earnings_revision_momentum: Optional[float] = None  # stub: TODO
    fu_analyst_estimate_dispersion: Optional[float] = None  # stub: TODO

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["feature_date"] = data["feature_date"].isoformat()
        data["ingested_at"] = data["ingested_at"].isoformat()
        return data

    def to_csv_dict(self) -> dict[str, Any]:
        data = self.model_dump(exclude={"ingested_at", "run_id"})
        data["feature_date"] = data["feature_date"].isoformat()
        return data


class EventRow(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    event_date: datetime.date
    event_type: EventType
    confidence_score: Optional[float] = None
    details: Optional[str] = None
    source: Source
    detection_run_id: str
    detected_at: datetime.datetime = Field(default_factory=utc_now)
    notes: Optional[str] = None

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["event_date"] = data["event_date"].isoformat()
        data["detected_at"] = data["detected_at"].isoformat()
        return data


class PipelineResult(BaseModel):
    symbols_processed: int = 0
    rows_written: int = 0
    errors: list[str] = Field(default_factory=list)

    @property
    def has_critical_errors(self) -> bool:
        return len(self.errors) > 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
