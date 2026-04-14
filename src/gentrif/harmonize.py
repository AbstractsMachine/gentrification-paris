"""
Harmonisation spatiale et temporelle.

L'extension spatio-temporelle des travaux de Clerval se heurte à deux
difficultés techniques que ce module doit adresser :

1. **Zonages IRIS évolutifs** : les codes IRIS changent périodiquement
   (révisions 2008, 2015, etc.). Une analyse longitudinale rigoureuse
   nécessite une table de passage entre millésimes. La référence la plus
   propre en open data est le dépôt Zenodo *"Harmonized INSEE
   socio-demographic IRIS-level data and IRIS conversion file (2010-2020)"*
   (cf. REFERENCES.bib).

2. **Ruptures de nomenclature CSP** : PCS 2003 (utilisée jusqu'en 2021)
   vs PCS 2020 (à partir de 2022). Les variables GSEC{XX}_{YY} de 2022
   agrègent actifs et retraités reclassés, ce que les bases publiques
   antérieures ne permettaient pas (retraités en CS7, monolithique).

Ce module expose les fonctions de **pivot long-format** (schéma canonique
`(year, geo_level, geo_code, geo_name, indicator, value)`) qui servent
de socle à toutes les analyses en aval, et prépare l'intégration future
des tables de passage IRIS.
"""
from __future__ import annotations

import pandas as pd

from .schemas import INDICATORS


# ---------------------------------------------------------------------------
# Pivot wide -> long (schéma canonique)
# ---------------------------------------------------------------------------
LONG_COLS = ["year", "geo_level", "geo_code", "geo_name", "indicator", "value"]


def to_long(df: pd.DataFrame, geo_level: str,
            geo_code_col: str, geo_name_col: str | None = None,
            indicators: list[str] | None = None) -> pd.DataFrame:
    """
    Pivote un DataFrame en format "tidy" long.

    Parameters
    ----------
    df : pd.DataFrame
        Doit contenir les colonnes de `indicators` et la colonne `year`.
    geo_level : str
        Un de {"iris", "quartier", "commune"}.
    geo_code_col : str
        Nom de la colonne contenant le code géographique (IRIS, num_quartier...).
    geo_name_col : str | None
        Nom de la colonne contenant le libellé, si dispo.
    indicators : list[str] | None
        Sous-ensemble des indicateurs à inclure. Par défaut tous ceux de
        `INDICATORS` présents dans `df`.

    Returns
    -------
    pd.DataFrame aux colonnes standardisées LONG_COLS.
    """
    inds = indicators or [i for i in INDICATORS if i in df.columns]
    records = []
    for _, row in df.iterrows():
        base = dict(
            year=int(row["year"]) if "year" in row else None,
            geo_level=geo_level,
            geo_code=str(row[geo_code_col]),
            geo_name=str(row[geo_name_col]) if geo_name_col and geo_name_col in row else None,
        )
        for ind in inds:
            val = row[ind]
            if pd.notna(val):
                records.append({**base, "indicator": ind, "value": float(val)})
    return pd.DataFrame(records, columns=LONG_COLS)


# ---------------------------------------------------------------------------
# Table de passage IRIS (stub)
# ---------------------------------------------------------------------------
def load_iris_crosswalk() -> pd.DataFrame | None:
    """
    Charge la table de passage IRIS inter-millésimes.

    TODO: intégrer la table de Zenodo (Eliot, 2023) permettant de rattacher
    chaque IRIS à son code historique/futur. Sans cette harmonisation, les
    comparaisons IRIS-à-IRIS 2007 vs 2022 sont approximatives pour les
    territoires redécoupés.

    Returns
    -------
    pd.DataFrame | None
        Colonnes attendues: iris_code, year_valid_from, year_valid_to,
        parent_code (pour agrégation) — ou None si non disponible.
    """
    return None


def harmonize_iris(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Applique la table de passage IRIS si disponible, sinon laisse tel quel
    en émettant un avertissement (à matérialiser dans les notebooks).
    """
    xwalk = load_iris_crosswalk()
    if xwalk is None:
        # Pas de crosswalk disponible — renvoyer tel quel.
        return df_long
    # TODO: implémenter le reclassement une fois la crosswalk en data/raw/.
    return df_long
