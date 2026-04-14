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
# Table de passage IRIS (Zenodo, cf. METHODOLOGY §4.4)
# ---------------------------------------------------------------------------
CROSSWALK_COLS = ("iris_src", "iris_dst", "weight")


def _normalise_crosswalk_cols(df: pd.DataFrame) -> pd.DataFrame | None:
    """Accepte plusieurs conventions de nommage et normalise vers
    (iris_src, iris_dst, weight) + éventuellement (year_src, year_dst)."""
    mapping = {}
    for src_candidate in ("iris_src", "IRIS_SRC", "iris_old", "CODE_IRIS_1",
                          "DCOMIRIS_ANC", "iris_from"):
        if src_candidate in df.columns:
            mapping[src_candidate] = "iris_src"
            break
    for dst_candidate in ("iris_dst", "IRIS_DST", "iris_new", "CODE_IRIS_2",
                          "DCOMIRIS", "iris_to"):
        if dst_candidate in df.columns:
            mapping[dst_candidate] = "iris_dst"
            break
    for w_candidate in ("weight", "WEIGHT", "poids", "share", "prop"):
        if w_candidate in df.columns:
            mapping[w_candidate] = "weight"
            break

    if not {"iris_src", "iris_dst"}.issubset(set(mapping.values())):
        return None

    out = df.rename(columns=mapping).copy()
    if "weight" not in out.columns:
        out["weight"] = 1.0
    for c in ("iris_src", "iris_dst"):
        out[c] = out[c].astype(str).str.strip()
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce").fillna(1.0)
    return out[[*CROSSWALK_COLS, *[c for c in ("year_src", "year_dst")
                                   if c in out.columns]]]


def load_iris_crosswalk() -> pd.DataFrame | None:
    """
    Charge la table de passage IRIS inter-millésimes depuis
    `data/raw/iris_crosswalk.csv` (déposé manuellement ou via
    `fetch.fetch_iris_crosswalk()`).

    Schéma normalisé en sortie : colonnes `iris_src`, `iris_dst`, `weight`
    (plus éventuellement `year_src`, `year_dst`). `weight` représente la
    part de l'IRIS source qui contribue à l'IRIS cible — utile pour une
    agrégation pondérée des effectifs quand un IRIS est scindé.

    Le loader tolère les conventions de nommage variables (Zenodo a publié
    plusieurs versions).
    """
    from .config import DATA_RAW, IRIS_CROSSWALK_FILENAME

    path = DATA_RAW / IRIS_CROSSWALK_FILENAME
    if not path.exists():
        return None

    for sep in (",", ";", "\t"):
        try:
            df = pd.read_csv(path, sep=sep, encoding="utf-8", dtype=str)
            if df.shape[1] >= 3:
                normalised = _normalise_crosswalk_cols(df)
                if normalised is not None:
                    return normalised
        except Exception:
            continue
    return None


def apply_crosswalk_wide(df_wide: pd.DataFrame,
                         crosswalk: pd.DataFrame,
                         iris_col: str = "IRIS",
                         count_cols: list[str] | None = None
                         ) -> pd.DataFrame:
    """
    Applique la table de passage à un DataFrame wide : chaque ligne de
    l'IRIS source contribue à l'IRIS cible selon son poids.

    Les colonnes de comptage (`count_cols`) sont pondérées par `weight`
    puis sommées par IRIS cible. Les autres colonnes (ratios, parts) sont
    recalculées en aval via `compute_indicators` — elles ne sont **pas**
    agrégeables linéairement.

    Parameters
    ----------
    df_wide : DataFrame avec colonne IRIS et les effectifs bruts.
    crosswalk : DataFrame normalisé (iris_src, iris_dst, weight).
    count_cols : liste des colonnes d'effectifs à pondérer. Par défaut,
        détecte automatiquement les clés CSP canoniques.
    """
    from .indicators import compute_indicators
    from .schemas import CSP_KEYS

    if count_cols is None:
        count_cols = [c for c in (*CSP_KEYS, "pop15p", "pop_fr", "pop_etr")
                      if c in df_wide.columns]
    if not count_cols:
        return df_wide

    merged = df_wide.merge(
        crosswalk[["iris_src", "iris_dst", "weight"]],
        left_on=iris_col, right_on="iris_src", how="left",
    )
    # IRIS sans mapping : identité (src = dst, weight = 1)
    merged["iris_dst"] = merged["iris_dst"].fillna(merged[iris_col])
    merged["weight"] = merged["weight"].fillna(1.0)

    for c in count_cols:
        merged[c] = merged[c] * merged["weight"]

    agg = merged.groupby("iris_dst", as_index=False)[count_cols].sum()
    agg = agg.rename(columns={"iris_dst": iris_col})
    if "year" in df_wide.columns:
        agg["year"] = df_wide["year"].iloc[0]
    return compute_indicators(agg)


def harmonize_iris(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Applique la table de passage IRIS au format long si disponible, sinon
    renvoie tel quel.

    Pour les indicateurs en format long, on ne peut pas agréger
    mécaniquement (un ratio n'est pas additif). On se contente d'annoter
    chaque ligne avec son `iris_dst` de référence pour permettre en aval
    une jointure vers les contours IRIS contemporains. L'harmonisation
    **additive** (effectifs bruts) se fait en amont via
    `apply_crosswalk_wide` sur les parquet `iris_wide_*`.
    """
    xwalk = load_iris_crosswalk()
    if xwalk is None:
        return df_long
    mapping = (xwalk.drop_duplicates("iris_src")
                     .set_index("iris_src")["iris_dst"])
    out = df_long.copy()
    if "geo_code" in out.columns:
        out["geo_code_harmonised"] = out["geo_code"].map(mapping).fillna(out["geo_code"])
    return out
