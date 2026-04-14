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
    (iris_src, iris_dst, weight, [year_src, year_dst])."""
    mapping = {}
    for src_candidate in ("iris_src", "IRIS_SRC", "iris_old", "IRIS_1",
                          "CODE_IRIS_1", "DCOMIRIS_ANC", "iris_from"):
        if src_candidate in df.columns:
            mapping[src_candidate] = "iris_src"
            break
    for dst_candidate in ("iris_dst", "IRIS_DST", "iris_new", "IRIS_2",
                          "CODE_IRIS_2", "DCOMIRIS", "iris_to"):
        if dst_candidate in df.columns:
            mapping[dst_candidate] = "iris_dst"
            break
    # Chabriel : `iris1_in_iris2_ajuste` = part de l'IRIS source dans
    # l'IRIS cible, ajustée — c'est exactement le poids d'agrégation.
    for w_candidate in ("weight", "WEIGHT", "poids", "share", "prop",
                        "iris1_in_iris2_ajuste", "iris1_in_iris2"):
        if w_candidate in df.columns:
            mapping[w_candidate] = "weight"
            break
    for ys_candidate in ("year_src", "year_1", "YEAR_SRC", "annee_1"):
        if ys_candidate in df.columns:
            mapping[ys_candidate] = "year_src"
            break
    for yd_candidate in ("year_dst", "year_2", "YEAR_DST", "annee_2"):
        if yd_candidate in df.columns:
            mapping[yd_candidate] = "year_dst"
            break

    if not {"iris_src", "iris_dst"}.issubset(set(mapping.values())):
        return None

    out = df.rename(columns=mapping).copy()
    if "weight" not in out.columns:
        out["weight"] = 1.0
    for c in ("iris_src", "iris_dst"):
        out[c] = out[c].astype(str).str.strip()
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce").fillna(1.0)
    cols = list(CROSSWALK_COLS) + [c for c in ("year_src", "year_dst")
                                    if c in out.columns]
    return out[cols]


def load_iris_crosswalk(source_year: int | None = None,
                        target_year: int | None = None,
                        dep_codes: list[str] | None = None,
                        ) -> pd.DataFrame | None:
    """
    Charge la table de passage IRIS inter-millésimes depuis
    `data/raw/iris_crosswalk.csv` (Zenodo / Chabriel 2024, couvre 1999-2023).

    Lecture par chunks pour gérer les fichiers volumineux (~700 Mo France
    entière). Filtrage à la lecture sur :
    - `source_year`/`target_year` — le mapping est récupéré dans les deux
      directions (Chabriel ne stocke que `year_src > year_dst`) et
      normalisé en sens forward (source → target) ;
    - préfixes de département (`dep_codes`) appliqués aux codes IRIS.

    Semantique des poids Chabriel :
    - `iris1_in_iris2_ajuste` = part de l'aire de IRIS_1 dans IRIS_2
    - `iris2_in_iris1_ajuste` = part de l'aire de IRIS_2 dans IRIS_1
    Pour agréger des effectifs source → target, on prend le poids qui
    représente la part du polygone **source** qui tombe dans le polygone
    **target** (« quelle fraction du source est incluse dans le target »).

    Schéma de sortie : `(iris_src, iris_dst, weight, year_src, year_dst)`
    avec `year_src = source_year`, `year_dst = target_year`.
    """
    from .config import DATA_INTERIM, DATA_RAW, IRIS_CROSSWALK_FILENAME

    path = DATA_RAW / IRIS_CROSSWALK_FILENAME
    if not path.exists():
        return None

    cache_key = f"{source_year or 'all'}_{target_year or 'all'}_" \
                f"{'-'.join(sorted(dep_codes)) if dep_codes else 'all'}"
    cache_path = DATA_INTERIM / f"iris_crosswalk_{cache_key}.parquet"
    if cache_path.exists():
        return pd.read_parquet(cache_path)

    # Sondage pour détecter format Chabriel vs format générique.
    probe = None
    sep = None
    for candidate in (";", ",", "\t"):
        try:
            p = pd.read_csv(path, sep=candidate, dtype=str, nrows=5)
            if p.shape[1] >= 3:
                probe = p
                sep = candidate
                break
        except Exception:
            continue
    if probe is None:
        return None

    is_chabriel = {"IRIS_1", "IRIS_2", "year_1", "year_2",
                   "iris1_in_iris2_ajuste",
                   "iris2_in_iris1_ajuste"}.issubset(set(probe.columns))

    dep_prefixes = tuple(dep_codes) if dep_codes else None
    kept: list[pd.DataFrame] = []

    if is_chabriel and source_year is not None and target_year is not None:
        src, tgt = str(source_year), str(target_year)
        use_cols = ["IRIS_1", "IRIS_2", "year_1", "year_2",
                    "iris1_in_iris2_ajuste", "iris2_in_iris1_ajuste"]
        reader = pd.read_csv(path, sep=sep, dtype=str, chunksize=500_000,
                             encoding="utf-8", usecols=use_cols)
        for chunk in reader:
            # Forward : year_1=src, year_2=tgt (rare — Chabriel ne stocke
            # que year_1 > year_2).
            fwd = chunk[(chunk["year_1"] == src) & (chunk["year_2"] == tgt)]
            if len(fwd):
                out = pd.DataFrame({
                    "iris_src": fwd["IRIS_1"],
                    "iris_dst": fwd["IRIS_2"],
                    "weight": pd.to_numeric(fwd["iris1_in_iris2_ajuste"],
                                            errors="coerce"),
                })
                if dep_prefixes:
                    out = out[out["iris_src"].str.startswith(dep_prefixes)]
                if len(out):
                    out["year_src"] = source_year
                    out["year_dst"] = target_year
                    kept.append(out)
            # Reverse : year_1=tgt, year_2=src → on inverse, poids =
            # iris2_in_iris1 (part du *src* dans le *tgt*).
            rev = chunk[(chunk["year_1"] == tgt) & (chunk["year_2"] == src)]
            if len(rev):
                out = pd.DataFrame({
                    "iris_src": rev["IRIS_2"],
                    "iris_dst": rev["IRIS_1"],
                    "weight": pd.to_numeric(rev["iris2_in_iris1_ajuste"],
                                            errors="coerce"),
                })
                if dep_prefixes:
                    out = out[out["iris_src"].str.startswith(dep_prefixes)]
                if len(out):
                    out["year_src"] = source_year
                    out["year_dst"] = target_year
                    kept.append(out)
    else:
        # Format générique via _normalise_crosswalk_cols
        reader = pd.read_csv(path, sep=sep, dtype=str, chunksize=500_000,
                             encoding="utf-8")
        for chunk in reader:
            norm = _normalise_crosswalk_cols(chunk)
            if norm is None:
                continue
            if source_year is not None and "year_src" in norm.columns:
                norm = norm[pd.to_numeric(norm["year_src"],
                                           errors="coerce") == source_year]
            if target_year is not None and "year_dst" in norm.columns:
                norm = norm[pd.to_numeric(norm["year_dst"],
                                           errors="coerce") == target_year]
            if dep_prefixes:
                norm = norm[norm["iris_src"].str.startswith(dep_prefixes)]
            if len(norm):
                kept.append(norm)

    if not kept:
        return None
    out = pd.concat(kept, ignore_index=True)
    out["weight"] = out["weight"].fillna(0.0)
    try:
        out.to_parquet(cache_path, index=False)
    except Exception:
        pass
    return out


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
    # Re-dérive les colonnes géographiques depuis le code IRIS cible :
    # IRIS à 9 chiffres = DEP(2) + COM(3) + reste(4). Pour Paris, COM
    # correspond aux arrondissements (75101..75120).
    if iris_col in agg.columns:
        codes = agg[iris_col].astype(str)
        agg["DEP"] = codes.str[:2]
        agg["COM"] = codes.str[:5]
        agg["ARRDT"] = agg["COM"].apply(
            lambda c: int(c[-2:]) if c.startswith("751") else 0
        )
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
