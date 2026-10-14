"""Shared configuration and path helpers for the final-project pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "project_config.yaml"


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_CONFIG
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    with config_path.open("r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    config["_config_path"] = str(config_path)
    return config


def project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def spark_path(value: str | Path) -> str:
    """Return an absolute Spark-friendly Windows path."""
    return project_path(value).resolve().as_posix()


def ensure_output_directories(config: dict[str, Any]) -> None:
    for key in ("metrics", "charts", "logs"):
        project_path(config["paths"][key]).mkdir(parents=True, exist_ok=True)


def postgres_password(config: dict[str, Any]) -> str | None:
    env_name = config["postgres"]["password_env"]
    return os.environ.get(env_name)
