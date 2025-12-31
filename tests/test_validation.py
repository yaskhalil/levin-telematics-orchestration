import pandas as pd

from include.validation.schema import validate_contract


def test_validate_contract_requires_columns() -> None:
    df = pd.DataFrame({"foo": [1]})
    try:
        validate_contract(df)
        assert False, "expected validate_contract to raise when required columns missing"
    except ValueError as e:
        assert "Missing required columns" in str(e)


def test_validate_contract_filters_invalid_rows_and_returns_breakdown() -> None:
    df = pd.DataFrame(
        {
            "timeStamp": ["2025-12-01T00:00:00Z", "not-a-ts", "2025-12-01T01:00:00Z"],
            "deviceID": ["dev1", "dev2", ""],
            "speed": [50, 10, 250],  # last row out of range, middle row timestamp invalid
            "rpm": [1000, 2000, 3000],
            "cTemp": [90, 95, 100],
        }
    )

    valid_df, result = validate_contract(df)

    assert result.total_rows == 3
    assert result.invalid_rows >= 2
    assert result.valid_rows == len(valid_df)
    assert "timestamp_parse_failed" in result.invalid_breakdown
    assert "device_id_null" in result.invalid_breakdown
    assert "speed_out_of_range" in result.invalid_breakdown


