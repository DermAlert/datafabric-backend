#!/usr/bin/env python3
"""Shared, dependency-light helpers for the dermatology experiment."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


EXPERIMENT_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = EXPERIMENT_DIR / "config"
RAW_DIR = EXPERIMENT_DIR / "raw"
DERIVED_DIR = EXPERIMENT_DIR / "derived"
RESULTS_DIR = EXPERIMENT_DIR / "results"
FIGURES_DIR = EXPERIMENT_DIR / "figures"
MANIFESTS_DIR = EXPERIMENT_DIR / "manifests"
LOGS_DIR = EXPERIMENT_DIR / "logs"

OUTPUT_DIRS = (
    RAW_DIR,
    DERIVED_DIR,
    RESULTS_DIR,
    FIGURES_DIR,
    MANIFESTS_DIR,
    LOGS_DIR,
)


def ensure_output_dirs() -> None:
    for directory in OUTPUT_DIRS:
        directory.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def source_config() -> dict[str, Any]:
    return load_json(CONFIG_DIR / "sources.json")


def mapping_config() -> dict[str, Any]:
    return load_json(CONFIG_DIR / "mappings.json")


def read_source(source: dict[str, Any]) -> pd.DataFrame:
    path = RAW_DIR / source["local_filename"]
    return pd.read_csv(path, low_memory=False)


def present_mask(series: pd.Series) -> pd.Series:
    """Treat empty strings and common textual null markers as missing."""
    text = series.astype("string").str.strip()
    return series.notna() & text.ne("") & ~text.str.lower().isin(
        {"na", "n/a", "nan", "none", "null", "unknown"}
    )


def stable_csv(df: pd.DataFrame, path: Path, sort_by: Iterable[str]) -> None:
    """Write a deterministic UTF-8 CSV suitable for checksum comparisons."""
    sort_columns = [column for column in sort_by if column in df.columns]
    output = df.sort_values(sort_columns, kind="stable") if sort_columns else df
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.10g")
