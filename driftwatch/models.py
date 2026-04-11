from __future__ import annotations

import datetime
from typing import Any, Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

EventType = Literal[
    "dividend",
    "capital_gain_dist",
    "split",
    "reverse_split",
    "rebalance",
    "benchmark_change",
    "expense_ratio_change",
    "aum_threshold",
    "manager_note",
]

Source = Literal["llm_auto", "manual"]


def utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


class OHLCVRow(BaseModel):
    symbol: str
    trade_date: datetime.date
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[int] = None
    avg_volume_30d: Optional[float] = None
    pe_ratio: Optional[float] = None
    adjusted_close: Optional[float] = None
    ingested_at: datetime.datetime = Field(default_factory=utc_now)
    data_source: str = "yfinance"

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
    net_assets: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    expense_ratio: Optional[float] = None
    category: Optional[str] = None
    fund_family: Optional[str] = None
    benchmark: Optional[str] = None
    ingested_at: datetime.datetime = Field(default_factory=utc_now)
    data_source: str = "yfinance"

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["snapshot_date"] = data["snapshot_date"].isoformat()
        data["ingested_at"] = data["ingested_at"].isoformat()
        return data


class EventRow(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    event_date: datetime.date
    event_type: EventType
    confidence_score: Optional[float] = None
    details: Optional[str] = None
    source: Source = ""
    detection_run_id: str
    detected_at: datetime.datetime = Field(default_factory=utc_now)
    notes: Optional[str] = None

    def to_bq_dict(self) -> dict[str, Any]:
        data = self.model_dump()
        data["event_date"] = data["event_date"].isoformat()
        data["detected_at"] = data["detected_at"].isoformat()
        return data


class PipelineResult(BaseModel):
    rows_written: int = 0
    errors: list[str] = Field(default_factory=list)

    @property
    def has_critical_errors(self) -> bool:
        return len(self.errors) > 0
