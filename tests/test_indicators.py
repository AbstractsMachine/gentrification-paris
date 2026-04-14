"""Tests des indicateurs canoniques."""
import numpy as np
import pandas as pd

from gentrif.indicators import (
    classify_level,
    classify_trajectory,
    compute_income_indicators,
    compute_indicators,
)


def _sample_frame():
    return pd.DataFrame({
        "cpis":       [30, 10, 50, 5, 0],
        "prof_inter": [20, 15, 15, 10, 0],
        "employes":   [20, 30, 10, 30, 0],
        "ouvriers":   [10, 30, 5, 35, 0],
        "pop15p":     [100, 100, 100, 100, 0],  # dernier cas pop nulle
        "pop_fr":     [80, 70, 85, 60, 0],
        "pop_etr":    [20, 30, 15, 40, 0],
    })


def test_compute_indicators_basic():
    df = compute_indicators(_sample_frame())
    assert (df["pct_cpis"].iloc[:4] == [30.0, 10.0, 50.0, 5.0]).all()
    assert (df["pct_classes_pop"].iloc[:4] == [30.0, 60.0, 15.0, 65.0]).all()


def test_ratio_gentrif_coherent():
    df = compute_indicators(_sample_frame())
    # ratio = pct_cpis / pct_classes_pop
    expected = [30 / 30, 10 / 60, 50 / 15, 5 / 65]
    np.testing.assert_allclose(df["ratio_gentrif"].iloc[:4].values,
                               [round(e, 3) for e in expected])


def test_pop_nulle_donne_nan():
    df = compute_indicators(_sample_frame())
    assert pd.isna(df["pct_cpis"].iloc[-1])
    assert pd.isna(df["ratio_gentrif"].iloc[-1])


def test_pct_etr_calculated_when_pop_etr_present():
    df = compute_indicators(_sample_frame())
    assert "pct_etr" in df.columns
    # 20 / (80+20) = 20%
    assert df["pct_etr"].iloc[0] == 20.0


def test_classify_level_labels():
    ratios = pd.Series([0.1, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0])
    labels = classify_level(ratios)
    # Tous doivent être labellisés (aucun None)
    assert labels.notna().sum() == len(ratios)
    # Le plus grand doit être en "Beaux quartiers"
    assert labels.iloc[-1] == "Beaux quartiers"


def test_classify_trajectory_four_cells():
    # 4 IRIS synthétiques, un par cellule du 2×2 :
    #   A : niveau bas  + hausse    → Gentrification
    #   B : niveau bas  + baisse    → Relégation
    #   C : niveau haut + hausse    → Consolidation bourgeoise
    #   D : niveau haut + baisse    → Déclassement
    t0 = pd.Series([0.3, 0.3, 3.0, 3.0], index=list("ABCD"))
    t1 = pd.Series([1.5, 0.2, 4.0, 2.0], index=list("ABCD"))
    labels = classify_trajectory(t0, t1)
    assert labels.loc["A"] == "Gentrification"
    assert labels.loc["B"] == "Relégation"
    assert labels.loc["C"] == "Consolidation bourgeoise"
    assert labels.loc["D"] == "Déclassement"


def test_compute_income_indicators_relative():
    # rel_med_uc est centré sur la médiane du périmètre.
    df = pd.DataFrame({"med_uc": [15_000, 20_000, 25_000, 30_000, 40_000]})
    df = compute_income_indicators(df)
    # médiane = 25_000 → rel_med_uc[2] == 1.0
    assert df["rel_med_uc"].iloc[2] == 1.0
    assert df["rel_med_uc"].iloc[-1] > 1.0
    assert df["rel_med_uc"].iloc[0] < 1.0


def test_compute_income_indicators_d9_d1():
    df = pd.DataFrame({
        "med_uc": [20_000, 30_000],
        "d1":     [10_000, 15_000],
        "d9":     [50_000, 60_000],
    })
    df = compute_income_indicators(df)
    assert "d9_d1" in df.columns
    assert df["d9_d1"].iloc[0] == 5.0


def test_classify_trajectory_nan_propagation():
    t0 = pd.Series([0.3, np.nan, 3.0], index=list("ABC"))
    t1 = pd.Series([1.5, 0.5, np.nan], index=list("ABC"))
    labels = classify_trajectory(t0, t1)
    assert labels.loc["A"] == "Gentrification"
    assert pd.isna(labels.loc["B"])
    assert pd.isna(labels.loc["C"])
