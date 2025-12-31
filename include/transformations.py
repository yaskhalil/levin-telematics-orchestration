from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


def normalize_telematics_df(
    df: pd.DataFrame,
    ds: str,
    source_file: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize source columns from v2.csv into the raw_telematics table shape.

    Notes:
    - Keeps only columns we load into Postgres.
    - Coerces numeric columns where present.
    - Adds partition_date, source_file metadata, and a lossless JSON payload.
    """
    work = df.copy()

    # canonical renames
    rename_map = {
        "tripID": "trip_id",
        "deviceID": "device_id",
        "accData": "acc_data",
        "cTemp": "coolant_temp_c",
        "dtc": "dtc_count",
        "eLoad": "engine_load",
        "iat": "intake_air_temp",
        "tAdv": "t_adv",
        "tPos": "t_pos",
    }
    work = work.rename(columns=rename_map)

    # Ensure event_time exists (validation step sets it; fallback if called standalone)
    if "event_time" not in work.columns and "timeStamp" in work.columns:
        work["event_time"] = pd.to_datetime(work["timeStamp"], errors="coerce")

    # In this dataset, device_id is treated as the vehicle identifier (README uses vehicle_id).
    work["vehicle_id"] = work.get("device_id")

    work["partition_date"] = ds

    if source_file:
        work["source_file"] = str(Path(source_file))
    else:
        work["source_file"] = None

    # numeric coercions where expected
    numeric_cols = [
        "gps_speed",
        "battery",
        "coolant_temp_c",
        "dtc_count",
        "engine_load",
        "intake_air_temp",
        "imap",
        "kpl",
        "maf",
        "rpm",
        "speed",
        "t_adv",
        "t_pos",
    ]
    for c in numeric_cols:
        if c in work.columns:
            work[c] = pd.to_numeric(work[c], errors="coerce")

    # Lossless payload for any downstream needs/debugging.
    # Store a JSON string so psycopg2 can adapt it easily to JSONB if desired.
    def _to_payload(row: pd.Series) -> str:
        d: Dict[str, Any] = row.to_dict()
        return json.dumps(d, default=str)

    work["payload"] = work.apply(_to_payload, axis=1)

    keep_cols = [
        "event_time",
        "partition_date",
        "vehicle_id",
        "device_id",
        "trip_id",
        "acc_data",
        "gps_speed",
        "battery",
        "coolant_temp_c",
        "dtc_count",
        "engine_load",
        "intake_air_temp",
        "imap",
        "kpl",
        "maf",
        "rpm",
        "speed",
        "t_adv",
        "t_pos",
        "source_file",
        "payload",
    ]
    existing = [c for c in keep_cols if c in work.columns]
    return work[existing].copy()


