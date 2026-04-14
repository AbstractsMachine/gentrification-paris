"""Tests des mappings de variables CSP par millésime."""
from gentrif.schemas import csp_vars


def test_pcs_2003_variables_2017():
    v = csp_vars(2017)
    assert v["cpis"] == "C17_POP15P_CS3"
    assert v["employes"] == "C17_POP15P_CS5"
    assert v["ouvriers"] == "C17_POP15P_CS6"
    assert v["pop15p"] == "P17_POP15P"


def test_pcs_2020_variables_2022():
    v = csp_vars(2022)
    # Retraités reclassés intégrés via _23 (cadres), _24 (PI), etc.
    assert v["cpis"] == "C22_POP15P_STAT_GSEC13_23"
    assert v["employes"] == "C22_POP15P_STAT_GSEC15_25"
    assert v["ouvriers"] == "C22_POP15P_STAT_GSEC16_26"
    # pop15p reconstruite par somme (pas de variable dédiée propre)
    assert v["pop15p"] is None


def test_rupture_2022_vs_2021():
    # 2021 doit encore utiliser l'ancienne nomenclature
    assert csp_vars(2021)["cpis"] == "C21_POP15P_CS3"
    # 2022 bascule
    assert "GSEC" in csp_vars(2022)["cpis"]
