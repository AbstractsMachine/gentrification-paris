"""
Chargeurs haut-niveau : de la source brute à un DataFrame enrichi
d'indicateurs.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from .config import DATA_RAW, QUARTIERS_PARIS, QUARTIER_YEARS
from .fetch import fetch_iris_contours, fetch_quartier_contours
from .indicators import compute_indicators
from .io import col_find, read_tabular
from .schemas import CSP_KEYS, csp_vars


# ---------------------------------------------------------------------------
# IRIS — bases Population INSEE
# ---------------------------------------------------------------------------
def load_iris(path: Path, year: int, dep_codes: list[str]) -> pd.DataFrame | None:
    """
    Charge une base IRIS INSEE, filtre par départements, calcule les
    indicateurs canoniques.

    Returns
    -------
    pd.DataFrame | None
        Colonnes garanties : IRIS, COM, DEP, pop15p, pct_cpis,
        pct_classes_pop, pct_prof_inter, ratio_gentrif, year.
    """
    df = read_tabular(path)
    if df is None:
        return None

    iris_c = col_find(df, "IRIS")
    com_c  = col_find(df, "COM") or col_find(df, "ARM") or col_find(df, "COM_ARM")

    if iris_c:
        df[iris_c] = df[iris_c].astype(str).str.strip()
        df = df[df[iris_c].str[:2].isin(dep_codes)]
    elif com_c:
        df[com_c] = df[com_c].astype(str).str.strip()
        df = df[df[com_c].str[:2].isin(dep_codes)]
    else:
        return None

    print(f"  -> {len(df)} IRIS dép. {','.join(dep_codes)}")

    out = pd.DataFrame()
    if iris_c:
        out["IRIS"] = df[iris_c].values
    if com_c:
        out["COM"] = df[com_c].astype(str).str.strip().values
    if "IRIS" in out:
        out["DEP"] = out["IRIS"].str[:2]
    elif "COM" in out:
        out["DEP"] = out["COM"].str[:2]
    if "COM" in out:
        out["ARRDT"] = out["COM"].apply(
            lambda x: int(x[-2:]) if x.startswith("751") else 0
        )

    for lc in ["LIBIRIS", "LIBCOM"]:
        fc = col_find(df, lc)
        if fc:
            out[lc] = df[fc].values

    vm = csp_vars(year)
    for key, var in vm.items():
        if not var:
            continue
        fc = col_find(df, var)
        out[key] = pd.to_numeric(df[fc], errors="coerce").fillna(0).values if fc else 0.0

    avail = [k for k in CSP_KEYS if k in out and out[k].sum() > 0]
    if "pop15p" not in out or out.get("pop15p", pd.Series([0])).sum() == 0:
        out["pop15p"] = out[avail].sum(axis=1) if avail else 0

    out = compute_indicators(out)
    out["year"] = year
    print(f"     CPIS={out['pct_cpis'].mean():.1f}%  "
          f"Ouv+Empl={out['pct_classes_pop'].mean():.1f}%")
    return out


# ---------------------------------------------------------------------------
# Contours géographiques
# ---------------------------------------------------------------------------
def load_iris_contours_gdf(dep_codes: list[str]) -> gpd.GeoDataFrame | None:
    path = fetch_iris_contours(dep_codes)
    if path is None:
        return None
    gdf = gpd.read_file(path)
    for c in ["iris_code", "CODE_IRIS", "code_iris", "DCOMIRIS"]:
        if c in gdf.columns:
            gdf["IRIS"] = gdf[c].astype(str).str.strip()
            break
    else:
        for c in gdf.columns:
            if gdf[c].dtype == object and \
               gdf[c].astype(str).str.match(r"^\d{9}$").sum() > 10:
                gdf["IRIS"] = gdf[c].astype(str).str.strip()
                break
    return gdf


def load_quartier_contours_gdf() -> gpd.GeoDataFrame | None:
    path = fetch_quartier_contours()
    if path is None:
        return None
    gdf = gpd.read_file(path)
    for c in gdf.columns:
        if gdf[c].dtype in [object, "int64", "float64"]:
            try:
                vals = pd.to_numeric(gdf[c], errors="coerce")
                if vals.between(1, 80).sum() >= 60:
                    gdf["num_quartier"] = vals.astype(int)
                    break
            except Exception:
                pass
    for c in gdf.columns:
        if "c_ar" in c.lower() or "arrond" in c.lower():
            gdf["arrondissement"] = pd.to_numeric(gdf[c], errors="coerce")
            break
    return gdf


# ---------------------------------------------------------------------------
# Module historique — 80 quartiers, données APUR
# ---------------------------------------------------------------------------
def quartier_template() -> pd.DataFrame:
    """CSV template à remplir manuellement depuis le Tableau 3 du PDF APUR."""
    rows = []
    for a, qs in QUARTIERS_PARIS.items():
        for n, nom in qs:
            rows.append(dict(num_quartier=n, nom=nom, arrondissement=a))
    df = pd.DataFrame(rows)
    for y in QUARTIER_YEARS:
        for c in ["cpis", "prof_inter", "employes", "ouvriers", "pop_totale"]:
            df[f"{c}_{y}"] = ""
    return df


def load_historical_quartiers() -> dict[int, pd.DataFrame]:
    """
    Charge les données CSP par quartier (1982, 1990, 1999) depuis un CSV
    utilisateur. Retourne un dict {year: DataFrame}.

    Le CSV attendu est `data/raw/quartiers_csp_data.csv` (séparateur `;`)
    avec les colonnes suivantes pour chaque année :
        cpis_{year}, prof_inter_{year}, employes_{year}, ouvriers_{year},
        pop_totale_{year}
    """
    for candidate in [DATA_RAW / "quartiers_csp_data.csv",
                      DATA_RAW / "quartiers_csp_template.csv"]:
        if not candidate.exists():
            continue
        df = pd.read_csv(candidate, sep=";", encoding="utf-8")
        results: dict[int, pd.DataFrame] = {}
        for y in QUARTIER_YEARS:
            cc = f"cpis_{y}"
            if cc not in df.columns:
                continue
            vals = pd.to_numeric(df[cc], errors="coerce")
            if vals.isna().all() or vals.sum() == 0:
                continue
            ydf = df[["num_quartier", "nom", "arrondissement"]].copy()
            for k in ["cpis", "prof_inter", "employes", "ouvriers", "pop_totale"]:
                ydf[k] = pd.to_numeric(df.get(f"{k}_{y}", 0),
                                       errors="coerce").fillna(0)
            # renomme pop_totale -> pop15p pour réutiliser compute_indicators
            ydf = ydf.rename(columns={"pop_totale": "pop15p"})
            ydf = compute_indicators(ydf)
            ydf["year"] = y
            results[y] = ydf
            print(f"  -> {y}: {len(ydf)} quartiers "
                  f"CPIS={ydf['pct_cpis'].mean():.1f}%")
        if results:
            return results
    return {}
