"""Tests du pivot long canonique et de la table de passage IRIS."""
import pandas as pd

from gentrif.harmonize import (
    LONG_COLS,
    _normalise_crosswalk_cols,
    apply_crosswalk_wide,
    to_long,
)


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


def test_crosswalk_normalise_aliases():
    df = pd.DataFrame({
        "iris_old": ["A", "B"],
        "iris_new": ["X", "X"],
        "poids": [0.5, 0.5],
    })
    out = _normalise_crosswalk_cols(df)
    assert out is not None
    assert set(["iris_src", "iris_dst", "weight"]).issubset(out.columns)
    assert out["iris_src"].tolist() == ["A", "B"]
    assert out["weight"].sum() == 1.0


def test_crosswalk_apply_wide_aggregates_weighted():
    # 2 IRIS sources fusionnés vers 1 IRIS cible, pondérés.
    wide = pd.DataFrame({
        "IRIS":     ["A", "B", "C"],
        "cpis":     [100.0, 200.0, 50.0],
        "employes": [10.0, 20.0, 5.0],
        "ouvriers": [5.0, 10.0, 2.5],
        "prof_inter": [0.0, 0.0, 0.0],
        "pop15p":   [115.0, 230.0, 57.5],
        "year":     [2022, 2022, 2022],
    })
    cw = pd.DataFrame({
        "iris_src": ["A", "B", "C"],
        "iris_dst": ["X", "X", "C"],
        "weight":   [0.5, 0.5, 1.0],
    })
    out = apply_crosswalk_wide(wide, cw, iris_col="IRIS")
    # X = 0.5*A + 0.5*B ; C reste C
    assert set(out["IRIS"]) == {"X", "C"}
    x = out[out["IRIS"] == "X"].iloc[0]
    assert x["cpis"] == 150.0   # 50 + 100
    # pct_cpis recalculé, pas juste moyenné
    assert "pct_cpis" in out.columns


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
