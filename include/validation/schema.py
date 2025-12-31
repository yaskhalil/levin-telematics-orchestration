from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValidationResult:
    total_rows: int
    valid_rows: int
    invalid_rows: int
    invalid_breakdown: Dict[str, int]


def validate_contract(df: pd.DataFrame) -> Tuple[pd.DataFrame, ValidationResult]:
    """
    Validate the Levin telematics contract using pandas-only rules.

    This intentionally avoids heavy dependencies (Pandera/GE) while still enforcing:
    - required columns exist
    - timestamp parses
    - device identifier non-null
    - basic range checks for numeric signals (when present)
    """
    required_cols = ["timeStamp", "deviceID"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    work = df.copy()

    invalid: Dict[str, pd.Series] = {}

    # timestamp parse check
    ts = pd.to_datetime(work["timeStamp"], errors="coerce")
    invalid["timestamp_parse_failed"] = ts.isna()
    work["event_time"] = ts

    # device identifier required
    dev = work["deviceID"].astype("string")
    invalid["device_id_null"] = dev.isna() | (dev.str.len() == 0)

    # optional numeric range checks
    def _numeric_range(col: str, lo: float, hi: float) -> None:
        if col not in work.columns:
            return
        s = pd.to_numeric(work[col], errors="coerce")
        # if present but non-numeric, treat as invalid (coerce -> NaN)
        invalid[f"{col}_non_numeric"] = s.isna() & work[col].notna()
        invalid[f"{col}_out_of_range"] = s.notna() & ((s < lo) | (s > hi))
        work[col] = s

    _numeric_range("speed", 0, 200)
    _numeric_range("rpm", 0, 10000)
    _numeric_range("cTemp", -40, 200)

    # Combine invalid rules into one mask
    any_invalid = pd.Series(False, index=work.index)
    breakdown: Dict[str, int] = {}
    for rule, mask in invalid.items():
        cnt = int(mask.sum())
        breakdown[rule] = cnt
        any_invalid = any_invalid | mask

    valid_df = work[~any_invalid].copy()
    result = ValidationResult(
        total_rows=int(len(work)),
        valid_rows=int(len(valid_df)),
        invalid_rows=int(any_invalid.sum()),
        invalid_breakdown=breakdown,
    )
    return valid_df, result


