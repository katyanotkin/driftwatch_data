from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_CONFIG_DIR = Path(__file__).parent.parent / "config"
_ENV_FILE = Path(__file__).parent.parent / ".env"


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment
    dw_env: str = Field(default="prod", alias="DW_ENV")

    # GCP — read from GCP_PROJECT_DW in .env or environment
    gcp_project_dw: str = Field(default="", alias="GCP_PROJECT_DW")
    gcp_project_id: str = Field(default="")   # resolved in validator
    gcp_dataset_id: str = Field(default="")   # derived from dw_env if not set
    gcp_location: str = Field(default="US")

    # Anthropic
    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")

    # Claude
    claude_model: str = Field(default="claude-sonnet-4-6")
    claude_max_tokens: int = Field(default=2048)
    claude_temperature: float = Field(default=0)

    # Pipelines
    ohlcv_lookback_days: int = Field(default=35)
    profile_snapshot_interval_days: int = Field(default=28)
    aum_minimum_usd: float = Field(default=100_000_000)
    min_confidence_score: float = Field(default=0.5)

    @model_validator(mode="after")
    def _resolve_from_yaml(self) -> "Settings":
        yaml_cfg = _load_yaml(_CONFIG_DIR / "settings.yaml")
        gcp = yaml_cfg.get("gcp", {})
        claude = yaml_cfg.get("claude", {})
        pipelines = yaml_cfg.get("pipelines", {})

        # Project ID: GCP_PROJECT_DW (.env / env var) > settings.yaml > placeholder
        if not self.gcp_project_id:
            self.gcp_project_id = (
                self.gcp_project_dw
                or gcp.get("project_id", "your-gcp-project")
            )

        # Dataset: settings.yaml override > derived from env
        if not self.gcp_dataset_id:
            self.gcp_dataset_id = (
                gcp.get("dataset_id") or f"driftwatch_{self.dw_env}"
            )

        if not self.gcp_location:
            self.gcp_location = gcp.get("location", "US")

        # Claude overrides from YAML (env vars still take priority via field defaults)
        if self.claude_model == "claude-sonnet-4-6":
            self.claude_model = claude.get("model", self.claude_model)
        if self.claude_max_tokens == 2048:
            self.claude_max_tokens = claude.get("max_tokens", self.claude_max_tokens)
        if self.claude_temperature == 0:
            self.claude_temperature = claude.get("temperature", self.claude_temperature)

        # Pipeline thresholds from YAML
        if self.ohlcv_lookback_days == 35:
            self.ohlcv_lookback_days = pipelines.get("ohlcv_lookback_days", 35)
        if self.profile_snapshot_interval_days == 28:
            self.profile_snapshot_interval_days = pipelines.get("profile_snapshot_interval_days", 28)
        if self.aum_minimum_usd == 100_000_000:
            self.aum_minimum_usd = pipelines.get("aum_minimum_usd", 100_000_000)
        if self.min_confidence_score == 0.5:
            self.min_confidence_score = pipelines.get("min_confidence_score", 0.5)

        return self

    @property
    def env(self) -> str:
        return self.dw_env


def load_symbols() -> list[dict[str, str]]:
    """Return list of {ticker, name, benchmark} dicts from symbols.yaml."""
    data = _load_yaml(_CONFIG_DIR / "symbols.yaml")
    return data.get("etfs", [])


def load_tickers() -> list[str]:
    return [e["ticker"] for e in load_symbols()]


def get_benchmark_map() -> dict[str, str]:
    """Return {ticker: benchmark} for override lookup."""
    return {e["ticker"]: e.get("benchmark", "") for e in load_symbols()}


# Module-level singleton
settings = Settings()
