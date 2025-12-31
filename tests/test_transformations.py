import json

import pandas as pd

from include.transformations import normalize_telematics_df


def test_normalize_telematics_df_maps_columns_and_adds_metadata() -> None:
    df = pd.DataFrame(
        {
            "timeStamp": ["2025-12-01T00:00:00Z"],
            "event_time": ["2025-12-01T00:00:00Z"],
            "tripID": ["t1"],
            "deviceID": ["d1"],
            "accData": ["x"],
            "cTemp": ["95"],
            "dtc": ["1"],
            "eLoad": ["0.5"],
            "iat": ["20"],
            "rpm": ["1000"],
            "speed": ["55"],
        }
    )

    out = normalize_telematics_df(df, ds="2025-12-01", source_file="data/v2.csv")

    assert out.loc[0, "device_id"] == "d1"
    assert out.loc[0, "vehicle_id"] == "d1"
    assert out.loc[0, "trip_id"] == "t1"
    assert out.loc[0, "partition_date"] == "2025-12-01"
    assert out.loc[0, "source_file"].endswith("data/v2.csv")

    # Numeric coercions should have happened
    assert float(out.loc[0, "coolant_temp_c"]) == 95.0
    assert int(out.loc[0, "dtc_count"]) == 1
    assert float(out.loc[0, "engine_load"]) == 0.5
    assert float(out.loc[0, "intake_air_temp"]) == 20.0
    assert float(out.loc[0, "rpm"]) == 1000.0
    assert float(out.loc[0, "speed"]) == 55.0

    # Payload should be valid JSON
    payload = json.loads(out.loc[0, "payload"])
    assert payload["deviceID"] == "d1"
    assert payload["tripID"] == "t1"


