"""Tests du pivot long canonique."""
import pandas as pd

from gentrif.harmonize import LONG_COLS, to_long


def test_to_long_schema_canonique():
    df = pd.DataFrame({
        "IRIS": ["751010101", "751010102"],
        "LIBIRIS": ["Q1", "Q2"],
        "year": [2022, 2022],
        "pct_cpis": [35.0, 15.0],
        "pct_classes_pop": [20.0, 55.0],
        "ratio_gentrif": [1.75, 0.27],
    })
    long = to_long(df, geo_level="iris",
                   geo_code_col="IRIS", geo_name_col="LIBIRIS")
    assert list(long.columns) == LONG_COLS
    # 2 lignes × 3 indicateurs = 6 records
    assert len(long) == 6
    assert set(long["indicator"].unique()) == {
        "pct_cpis", "pct_classes_pop", "ratio_gentrif",
    }
    assert (long["geo_level"] == "iris").all()


def test_to_long_skip_nan():
    df = pd.DataFrame({
        "IRIS": ["A", "B"],
        "year": [2022, 2022],
        "pct_cpis": [30.0, float("nan")],
    })
    long = to_long(df, geo_level="iris", geo_code_col="IRIS",
                   indicators=["pct_cpis"])
    # Les NaN ne sont pas émis
    assert len(long) == 1
    assert long.iloc[0]["geo_code"] == "A"
