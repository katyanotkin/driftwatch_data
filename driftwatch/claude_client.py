from __future__ import annotations

import datetime
import json
import logging
from typing import Any

import anthropic

from driftwatch.models import EventRow
from driftwatch.settings import settings

log = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a financial data analyst monitoring ETF market events.
You will be given data for a single ETF and must identify any significant events that occurred.

You must respond with valid JSON only — no prose, no markdown fences.
The response must be a JSON array of event objects. If no events are detected, return an empty array [].

Each event object must have exactly these fields:
{
  "event_type": one of ["dividend", "capital_gain_dist", "split", "reverse_split",
                         "rebalance", "benchmark_change", "expense_ratio_change",
                         "aum_threshold", "manager_note"],
  "event_date": "YYYY-MM-DD",
  "confidence_score": float between 0.0 and 1.0,
  "details": { ...freeform key-value pairs relevant to the event... },
  "notes": "human readable explanation of why this event was detected"
}

Detection rules:
- split: today_close / yesterday_close < 0.6 AND today_volume > 2x avg_volume_30d
- reverse_split: today_close / yesterday_close > 1.5 AND today_volume > 2x avg_volume_30d
- dividend: close drops 2–8% AND volume is elevated (>1.5x avg), typically not a general market move
- capital_gain_dist: similar to dividend but typically occurs November–December
- expense_ratio_change: expense_ratio in current_profile differs from previous_profile by any amount
- benchmark_change: benchmark field in current_profile differs from previous_profile (non-null both sides)
- aum_threshold: current net_assets < 100000000 (i.e. below $100 million)
- rebalance: volume spike >3x avg with moderate price stability (price change < 2%) — flag for review

Confidence scoring:
- 1.0: mathematical certainty (e.g., expense_ratio field literally changed value)
- 0.8–0.9: strong signal (price ratio matches known split ratios 2:1, 3:1, 1:2, etc.)
- 0.5–0.7: moderate signal (price drop consistent with dividend but uncertain)
- Below 0.5: do not emit the event

Emit only events with confidence_score >= 0.5.
"""


class ClaudeClient:
    def __init__(self) -> None:
        self._client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    def detect_events(
        self,
        payload: dict[str, Any],
        symbol: str,
        run_id: str,
    ) -> list[EventRow]:
        """Call Claude with an OHLCV or profile comparison payload.

        Returns a list of EventRow objects (may be empty).
        Logs and returns [] on any error.
        """
        try:
            response = self._client.messages.create(
                model=settings.claude_model,
                max_tokens=settings.claude_max_tokens,
                temperature=settings.claude_temperature,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": json.dumps(payload)}],
            )
        except Exception as exc:
            log.error("%s: Claude API call failed: %s", symbol, exc)
            return []

        raw_text = response.content[0].text.strip()
        return _parse_events(raw_text, symbol, run_id)


def _parse_events(raw_text: str, symbol: str, run_id: str) -> list[EventRow]:
    # Strip accidental markdown fences
    text = raw_text
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    try:
        raw_events: list[dict[str, Any]] = json.loads(text)
    except json.JSONDecodeError as exc:
        log.warning("%s: Claude returned invalid JSON (%s). Raw: %s", symbol, exc, raw_text[:200])
        return []

    if not isinstance(raw_events, list):
        log.warning("%s: Claude response is not a list. Got: %s", symbol, type(raw_events))
        return []

    events: list[EventRow] = []
    for e in raw_events:
        try:
            score = float(e.get("confidence_score", 0))
            if score < settings.min_confidence_score:
                continue

            details_raw = e.get("details")
            details_str = json.dumps(details_raw) if details_raw else None

            event_date_str = e.get("event_date")
            event_date = datetime.date.fromisoformat(event_date_str) if event_date_str else datetime.date.today()

            events.append(
                EventRow(
                    symbol=symbol,
                    event_date=event_date,
                    event_type=e["event_type"],
                    confidence_score=score,
                    details=details_str,
                    source="auto",
                    detection_run_id=run_id,
                    notes=e.get("notes"),
                )
            )
        except Exception as exc:
            log.warning("%s: skipping malformed event entry (%s): %s", symbol, exc, e)

    return events


# Module-level singleton
claude_client = ClaudeClient()
