"""Tests des indicateurs canoniques."""
import numpy as np
import pandas as pd

from gentrif.indicators import classify_gentrification, compute_indicators


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


def test_classify_gentrification_labels():
    ratios = pd.Series([0.1, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0])
    labels = classify_gentrification(ratios)
    # Tous doivent être labellisés (aucun None)
    assert labels.notna().sum() == len(ratios)
    # Le plus grand doit être en "Beaux quartiers"
    assert labels.iloc[-1] == "Beaux quartiers"
