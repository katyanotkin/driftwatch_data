from __future__ import annotations

import datetime
import json
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

Source = Literal["claude_auto", "manual"]


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
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    data_source: str = "yfinance"

    def to_bq_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        d["trade_date"] = d["trade_date"].isoformat()
        d["ingested_at"] = d["ingested_at"].isoformat()
        return d


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
    ingested_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    data_source: str = "yfinance"

    def to_bq_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        d["snapshot_date"] = d["snapshot_date"].isoformat()
        d["ingested_at"] = d["ingested_at"].isoformat()
        return d


class EventRow(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    event_date: datetime.date
    event_type: EventType
    confidence_score: Optional[float] = None
    details: Optional[str] = None  # JSON string
    source: Source = "claude_auto"
    detection_run_id: str = ""
    detected_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    notes: Optional[str] = None

    def to_bq_dict(self) -> dict[str, Any]:
        d = self.model_dump()
        d["event_date"] = d["event_date"].isoformat()
        d["detected_at"] = d["detected_at"].isoformat()
        return d


class PipelineResult(BaseModel):
    rows_written: int = 0
    errors: list[str] = Field(default_factory=list)

    @property
    def has_critical_errors(self) -> bool:
        return len(self.errors) > 0
