"""
Chargeurs haut-niveau : de la source brute à un DataFrame enrichi
d'indicateurs.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from .config import (
    DATA_RAW,
    LONG_SERIES_YEARS,
    MIN_POP_ACTIVE,
    NON_RESIDENTIAL_KEYWORDS,
    QUARTIERS_PARIS,
    QUARTIER_YEARS,
)
from .fetch import (
    fetch_commune_contours,
    fetch_iris_contours,
    fetch_quartier_contours,
)
from .indicators import compute_income_indicators, compute_indicators
from .io import col_find, read_tabular
from .schemas import CSP_KEYS, csp_long_vars, csp_vars, filosofi_vars


# ---------------------------------------------------------------------------
# Filtrage IRIS non-résidentiels (bois, cimetières, grands équipements)
# ---------------------------------------------------------------------------
def _non_residential_mask(out: pd.DataFrame) -> pd.Series:
    """Identifie les IRIS à écarter : pop < seuil OU libellé correspondant.

    Cf. config.MIN_POP_ACTIVE et config.NON_RESIDENTIAL_KEYWORDS.
    """
    mask = pd.Series(False, index=out.index)
    if "pop15p" in out.columns:
        mask |= pd.to_numeric(out["pop15p"], errors="coerce").fillna(0) < MIN_POP_ACTIVE
    if "LIBIRIS" in out.columns:
        lib_u = out["LIBIRIS"].astype(str).str.upper()
        for kw in NON_RESIDENTIAL_KEYWORDS:
            mask |= lib_u.str.contains(kw, na=False, regex=False)
    return mask


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

    nr_mask = _non_residential_mask(out)
    out["non_residential"] = nr_mask
    if nr_mask.any():
        cols_to_mask = [c for c in ("pct_cpis", "pct_classes_pop",
                                     "pct_prof_inter", "ratio_gentrif")
                        if c in out.columns]
        out.loc[nr_mask, cols_to_mask] = np.nan
        print(f"     {int(nr_mask.sum())} IRIS non-résidentiels masqués "
              f"(pop<{MIN_POP_ACTIVE} ou libellé bois/cimetière/équipement)")

    out["year"] = year
    print(f"     CPIS={out['pct_cpis'].mean():.1f}%  "
          f"Ouv+Empl={out['pct_classes_pop'].mean():.1f}%")
    return out


# ---------------------------------------------------------------------------
# IRIS — bases FiLoSoFi (revenus fiscaux disponibles par UC)
# ---------------------------------------------------------------------------
def load_filosofi_iris(path: Path, year: int,
                       dep_codes: list[str]) -> pd.DataFrame | None:
    """
    Charge une base FiLoSoFi IRIS, filtre par départements, calcule les
    indicateurs de revenus.

    Colonnes garanties : IRIS, DEP, med_uc, d1, d9, poverty_rate, gini,
    rel_med_uc, d9_d1, year. rel_med_uc est le rapport entre la médiane
    de l'IRIS et la médiane du périmètre chargé (= centré sur 1.0).
    """
    df = read_tabular(path)
    if df is None:
        return None

    iris_c = col_find(df, "IRIS")
    if not iris_c:
        return None
    df[iris_c] = df[iris_c].astype(str).str.strip()
    df = df[df[iris_c].str[:2].isin(dep_codes)]
    if len(df) == 0:
        return None

    out = pd.DataFrame({"IRIS": df[iris_c].values})
    out["DEP"] = out["IRIS"].str[:2]
    com_c = col_find(df, "COM")
    if com_c:
        out["COM"] = df[com_c].astype(str).str.strip().values

    vmap = filosofi_vars(year)
    for key, candidates in vmap.items():
        found = None
        for c in candidates:
            if c in df.columns:
                found = c
                break
        if found is None:
            for col in df.columns:
                cu = col.upper().replace("-", "").replace("_", "")
                if key == "med_uc" and "MED" in cu and str(year % 100) in cu:
                    found = col; break
                if key == "d1" and cu.endswith(f"D1{year%100:02d}"):
                    found = col; break
                if key == "d9" and cu.endswith(f"D9{year%100:02d}"):
                    found = col; break
                if key == "poverty_rate" and "TP60" in cu:
                    found = col; break
                if key == "gini" and ("GI" in cu and str(year % 100) in cu):
                    found = col; break
        out[key] = (pd.to_numeric(df[found], errors="coerce").values
                    if found else np.nan)

    out = compute_income_indicators(out)
    out["year"] = year
    if out["med_uc"].notna().any():
        print(f"  -> {len(out)} IRIS FiLoSoFi {year} "
              f"dép.{','.join(dep_codes)} | med_uc médian="
              f"{out['med_uc'].median():.0f}€")
    return out


# ---------------------------------------------------------------------------
# Contours géographiques
# ---------------------------------------------------------------------------
def _clean_iris_code(raw) -> str:
    """Extrait un code IRIS à 9 chiffres depuis diverses sérialisations
    (str, liste Python, JSON array sérialisé en chaîne)."""
    import re
    if isinstance(raw, list) and raw:
        raw = raw[0]
    s = str(raw).strip()
    m = re.search(r"\d{9}", s)
    return m.group(0) if m else s


def load_iris_contours_gdf(dep_codes: list[str]) -> gpd.GeoDataFrame | None:
    path = fetch_iris_contours(dep_codes)
    if path is None:
        return None
    gdf = gpd.read_file(path)

    # Les contours opendatasoft exposent une colonne `year` qui désigne
    # le millésime du zonage — sans rapport avec l'année des données
    # statistiques. On la retire pour éviter les collisions lors des merges.
    for drop_col in ("year", "YEAR"):
        if drop_col in gdf.columns:
            gdf = gdf.drop(columns=drop_col)

    # Les contours opendatasoft stockent iris_code comme liste JSON
    # (["751135119"]). On normalise systématiquement.
    for c in ["iris_code", "CODE_IRIS", "code_iris", "DCOMIRIS"]:
        if c in gdf.columns:
            gdf["IRIS"] = gdf[c].map(_clean_iris_code)
            return gdf

    # Fallback : chercher une colonne contenant des codes à 9 chiffres
    for c in gdf.columns:
        if gdf[c].dtype == object:
            candidate = gdf[c].map(_clean_iris_code)
            if candidate.str.match(r"^\d{9}$").sum() > 10:
                gdf["IRIS"] = candidate
                return gdf
    return gdf


def load_commune_contours_gdf(dep_codes: list[str]) -> gpd.GeoDataFrame | None:
    """Contours GeoJSON des communes pour un périmètre départemental.

    Paris apparaît comme une seule commune (75056) dans ce fichier ; les
    20 arrondissements sont en fallback dans les contours IRIS (via COM).
    """
    path = fetch_commune_contours(dep_codes)
    if path is None:
        return None
    gdf = gpd.read_file(path)
    for drop_col in ("year", "YEAR"):
        if drop_col in gdf.columns:
            gdf = gdf.drop(columns=drop_col)

    def _clean_com(raw) -> str:
        import re
        if isinstance(raw, list) and raw:
            raw = raw[0]
        m = re.search(r"\d{5}", str(raw))
        return m.group(0) if m else str(raw).strip()

    for c in ("com_code", "codgeo", "CODGEO", "insee_com"):
        if c in gdf.columns:
            gdf["CODGEO"] = gdf[c].map(_clean_com)
            break
    if "CODGEO" not in gdf.columns:
        for c in gdf.columns:
            if gdf[c].dtype == object:
                vals = gdf[c].map(_clean_com)
                if vals.str.match(r"^\d{5}$").sum() > 10:
                    gdf["CODGEO"] = vals
                    break

    # Paris apparaît comme une commune unique (75056) dans ce fichier ;
    # les données INSEE longues utilisent les 20 codes d'arrondissement
    # (75101-75120). On complète à partir des contours IRIS dissous par
    # les 5 premiers chiffres de IRIS (= COM de l'arrondissement).
    if "75" in dep_codes and (gdf["CODGEO"] == "75056").any():
        iris_gdf = load_iris_contours_gdf(["75"])
        if iris_gdf is not None and "IRIS" in iris_gdf.columns:
            iris_gdf = iris_gdf.copy()
            iris_gdf["CODGEO"] = iris_gdf["IRIS"].astype(str).str[:5]
            arr = iris_gdf[iris_gdf["CODGEO"].str.startswith("751")]
            arr = arr[["CODGEO", "geometry"]].dissolve(by="CODGEO").reset_index()
            if len(arr):
                target_crs = arr.crs or gdf.crs
                gdf = gdf[gdf["CODGEO"] != "75056"]
                gdf = pd.concat([gdf[["CODGEO", "geometry"]], arr],
                                ignore_index=True)
                gdf = gpd.GeoDataFrame(gdf, geometry="geometry",
                                       crs=target_crs)

    return gdf


def load_quartier_contours_gdf() -> gpd.GeoDataFrame | None:
    """Contours des 80 quartiers administratifs de Paris (opendata.paris.fr).

    Le champ `c_qu` contient le numéro officiel du quartier (1 à 80) ;
    on le copie dans `num_quartier` pour compatibilité avec le reste du
    pipeline.
    """
    path = fetch_quartier_contours()
    if path is None:
        return None
    gdf = gpd.read_file(path)
    for c in ("c_qu", "num_quartier"):
        if c in gdf.columns:
            vals = pd.to_numeric(gdf[c], errors="coerce")
            if vals.between(1, 80).sum() >= 60:
                gdf["num_quartier"] = vals.fillna(0).astype(int)
                break
    for c in gdf.columns:
        if "c_ar" in c.lower() or "arrond" in c.lower():
            gdf["arrondissement"] = pd.to_numeric(gdf[c], errors="coerce")
            break
    return gdf


# ---------------------------------------------------------------------------
# Séries longues INSEE — communes, 1968-2022 (page 1893185)
# ---------------------------------------------------------------------------
def load_long_series(path: Path, dep_codes: list[str]) -> pd.DataFrame:
    """
    Charge les séries harmonisées INSEE "Population active 25-54 ans par
    CSP" (fichier `pop-act2554-csp-cd-*.xlsx`, page 1893185).

    Structure attendue : une feuille par (niveau, année), nommée
    `COM_{year}` pour le niveau communal. En-tête multi-lignes :
    - ligne 12 : numéro de CS (1..8)
    - ligne 13 : TYPE_ACTIVITE (1 = actif ayant un emploi, 2 = chômeur)
    - ligne 14 : libellé français
    - ligne 15 : codes courts (RR, DR, CR, STABLE, DR24, LIBELLE, CSx_y…)
    - ligne 16+ : données

    Le code commune à 5 chiffres est reconstruit depuis DR (département)
    + CR (commune). Pour chaque CS, on somme TYPE_ACTIVITE 1 et 2.
    """
    if path.suffix not in (".xls", ".xlsx"):
        return pd.DataFrame()

    try:
        xl = pd.ExcelFile(path)
    except Exception:
        return pd.DataFrame()

    rows = []
    for year in LONG_SERIES_YEARS:
        sheet = f"COM_{year}"
        if sheet not in xl.sheet_names:
            continue

        # Lecture brute pour extraire les headers multi-lignes
        raw = pd.read_excel(path, sheet_name=sheet, header=None)
        cs_row = raw.iloc[12].tolist()      # numéro de CS
        type_row = raw.iloc[13].tolist()    # type d'activité
        code_row = raw.iloc[15].tolist()    # codes courts
        data = raw.iloc[16:].copy()
        data.columns = range(data.shape[1])

        # Localiser les colonnes d'identification
        idx = {code: i for i, code in enumerate(code_row) if pd.notna(code)}
        dr_i = idx.get("DR"); cr_i = idx.get("CR"); lib_i = idx.get("LIBELLE")
        if dr_i is None or cr_i is None:
            continue

        df_year = pd.DataFrame({
            "DR": data[dr_i].astype(str).str.strip(),
            "CR": data[cr_i].astype(str).str.strip(),
        })
        df_year["CODGEO"] = df_year["DR"].str.zfill(2) + df_year["CR"].str.zfill(3)
        df_year["DEP"] = df_year["CODGEO"].str[:2]
        df_year["LIBGEO"] = (data[lib_i].astype(str).values
                             if lib_i is not None else "")
        df_year = df_year[df_year["DEP"].isin(dep_codes)].copy()
        if len(df_year) == 0:
            continue

        # Agrégation CSP : somme TYPE 1 + TYPE 2 par CS
        cs_to_key = {3: "cpis", 4: "prof_inter", 5: "employes", 6: "ouvriers"}
        for cs_num, key in cs_to_key.items():
            cols = [i for i in range(len(cs_row))
                    if str(cs_row[i]).strip() in (str(cs_num), f"{cs_num}.0")]
            if not cols:
                df_year[key] = 0.0
                continue
            values = data[cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            df_year[key] = values.sum(axis=1).loc[df_year.index].values

        df_year["pop_act"] = df_year[["cpis", "prof_inter",
                                       "employes", "ouvriers"]].sum(axis=1)
        df_year["pop15p"] = df_year["pop_act"]
        df_year = compute_indicators(df_year)
        df_year["year"] = year
        df_year = df_year[df_year["pop_act"] > 0]

        rows.append(df_year[["year", "CODGEO", "DEP", "LIBGEO", "cpis",
                             "prof_inter", "employes", "ouvriers",
                             "pop_act", "pop15p", "pct_cpis",
                             "pct_classes_pop", "pct_prof_inter",
                             "ratio_gentrif"]])
        print(f"  -> {year}: {len(df_year)} communes "
              f"dép.{','.join(dep_codes)} "
              f"CPIS={df_year['pct_cpis'].mean():.1f}% "
              f"ratio={df_year['ratio_gentrif'].mean():.2f}")

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


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
