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

    dw_env: str = Field(default="prod", alias="DW_ENV")
    gcp_project: str = Field(default="", alias="GCP_PROJECT")
    gcp_project_id: str = Field(default="")   # resolved in validator
    gcp_dataset_id: str = Field(default="")   # derived in validator
    gcp_location: str = Field(default="US")

    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")

    claude_model: str = Field(default="claude-sonnet-4-6")
    claude_max_tokens: int = Field(default=2048)

    history_days: int = Field(default=252)
    min_confidence_score: float = Field(default=0.5)

    @model_validator(mode="after")
    def _resolve(self) -> "Settings":
        if not self.gcp_project_id:
            self.gcp_project_id = self.gcp_project or "your-gcp-project"
        if not self.gcp_dataset_id:
            self.gcp_dataset_id = f"sigforge_{self.dw_env}"
        return self

    @property
    def env(self) -> str:
        return self.dw_env


def load_symbols() -> list[dict[str, str]]:
    """Return list of stock dicts from config/symbols.yaml."""
    data = _load_yaml(_CONFIG_DIR / "symbols.yaml")
    return data.get("stocks", [])


def load_tickers() -> list[str]:
    return [s["ticker"] for s in load_symbols()]


def get_sector_map() -> dict[str, str]:
    """Return {ticker: gics_sector} from symbols.yaml."""
    return {s["ticker"]: s.get("gics_sector", "") for s in load_symbols()}


settings = Settings()
