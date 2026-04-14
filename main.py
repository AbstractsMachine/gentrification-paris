#!/usr/bin/env python3
"""
Gentrification Paris & Grand Paris
Actualisation de Clerval A. (2010), Cybergeo n°505

1982-1999 : 80 quartiers (APUR) | 2007-2022 : IRIS (INSEE)
Paris intra-muros + Petite couronne (92, 93, 94)
"""

import os, sys, glob, re, zipfile, warnings, argparse
from pathlib import Path

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import requests

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.patches import Patch

# ==========================================================================
# CONFIG
# ==========================================================================
DATA = Path("data")
OUT  = Path("output")
DATA.mkdir(exist_ok=True)
OUT.mkdir(exist_ok=True)

DEPS_PARIS = ["75"]
DEPS_GP    = ["75", "92", "93", "94"]

SCOPES = {
    "paris":       (DEPS_PARIS, "Paris intra-muros"),
    "grand_paris": (DEPS_GP,    "Petite couronne"),
}

INSEE_PAGES = {2007: "2028650", 2012: "2028582", 2017: "4799309", 2022: "8647014"}

QUARTIERS = {a: [(n, q) for n, q in qs] for a, qs in {
    1:  [(1, "St-Germain-l'Auxerrois"), (2, "Halles"), (3, "Palais-Royal"), (4, "Pl. Vendôme")],
    2:  [(5, "Gaillon"), (6, "Vivienne"), (7, "Mail"), (8, "Bonne-Nouvelle")],
    3:  [(9, "Arts-et-Métiers"), (10, "Enfants-Rouges"), (11, "Archives"), (12, "Ste-Avoye")],
    4:  [(13, "St-Merri"), (14, "St-Gervais"), (15, "Arsenal"), (16, "Notre-Dame")],
    5:  [(17, "St-Victor"), (18, "Jardin-des-Plantes"), (19, "Val-de-Grâce"), (20, "Sorbonne")],
    6:  [(21, "Monnaie"), (22, "Odéon"), (23, "ND-des-Champs"), (24, "St-Germain-des-Prés")],
    7:  [(25, "St-Thomas-d'Aquin"), (26, "Invalides"), (27, "École-Militaire"), (28, "Gros-Caillou")],
    8:  [(29, "Champs-Élysées"), (30, "Fbg-du-Roule"), (31, "Madeleine"), (32, "Europe")],
    9:  [(33, "St-Georges"), (34, "Chaussée-d'Antin"), (35, "Fbg-Montmartre"), (36, "Rochechouart")],
    10: [(37, "St-Vincent-de-Paul"), (38, "Porte-St-Denis"), (39, "Porte-St-Martin"), (40, "Hôp.-St-Louis")],
    11: [(41, "Folie-Méricourt"), (42, "St-Ambroise"), (43, "Roquette"), (44, "Ste-Marguerite")],
    12: [(45, "Bel-Air"), (46, "Picpus"), (47, "Bercy"), (48, "Quinze-Vingts")],
    13: [(49, "Salpêtrière"), (50, "Gare"), (51, "Maison-Blanche"), (52, "Croulebarbe")],
    14: [(53, "Montparnasse"), (54, "Parc-Montsouris"), (55, "Petit-Montrouge"), (56, "Plaisance")],
    15: [(57, "St-Lambert"), (58, "Necker"), (59, "Grenelle"), (60, "Javel")],
    16: [(61, "Auteuil"), (62, "Muette"), (63, "Porte-Dauphine"), (64, "Chaillot")],
    17: [(65, "Ternes"), (66, "Plaine-Monceaux"), (67, "Batignolles"), (68, "Épinettes")],
    18: [(69, "Grandes-Carrières"), (70, "Clignancourt"), (71, "Goutte-d'Or"), (72, "Chapelle")],
    19: [(73, "Villette"), (74, "Pont-de-Flandre"), (75, "Amérique"), (76, "Combat")],
    20: [(77, "Belleville"), (78, "St-Fargeau"), (79, "Père-Lachaise"), (80, "Charonne")],
}.items()}


def csp_vars(year):
    """Variable names for CSP by census year."""
    yy = f"{year % 100:02d}"
    if year <= 2021:
        return dict(cpis=f"C{yy}_POP15P_CS3", prof_inter=f"C{yy}_POP15P_CS4",
                    employes=f"C{yy}_POP15P_CS5", ouvriers=f"C{yy}_POP15P_CS6",
                    retraites=f"C{yy}_POP15P_CS7", sans_act=f"C{yy}_POP15P_CS8",
                    artisans=f"C{yy}_POP15P_CS2", pop15p=f"P{yy}_POP15P",
                    pop_fr=f"P{yy}_POP_FR", pop_etr=f"P{yy}_POP_ETR")
    return dict(cpis=f"C{yy}_POP15P_STAT_GSEC13_23", prof_inter=f"C{yy}_POP15P_STAT_GSEC14_24",
                employes=f"C{yy}_POP15P_STAT_GSEC15_25", ouvriers=f"C{yy}_POP15P_STAT_GSEC16_26",
                retraites=f"C{yy}_POP15P_STAT_GSEC32", sans_act=f"C{yy}_POP15P_STAT_GSEC40",
                artisans=f"C{yy}_POP15P_STAT_GSEC12_22", pop15p=None,
                pop_fr=f"P{yy}_POP_FR", pop_etr=f"P{yy}_POP_ETR")


# ==========================================================================
# DOWNLOAD HELPERS
# ==========================================================================
def fetch(url, dest, desc=""):
    if dest.exists():
        print(f"  [ok] {desc or dest.name} (cache)")
        return True
    print(f"  [dl] {desc or url[:80]}...")
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        dest.write_bytes(r.content)
        print(f"       {len(r.content)/1024/1024:.1f} Mo")
        return True
    except Exception as e:
        print(f"  [x]  {e}")
        return False


def find_data_file(year):
    """Find or download INSEE IRIS data for a given year."""
    for pat in [f"*pop*{year}*.csv", f"*{year}*pop*.csv", f"*{year}*.csv",
                f"*pop*{year}*.xls*", f"*{year}*.xls*"]:
        hits = sorted(DATA.glob(pat), key=lambda p: p.stat().st_size, reverse=True)
        if hits:
            print(f"  [ok] {year}: {hits[0].name}")
            return hits[0]

    pid = INSEE_PAGES.get(year)
    if not pid:
        return None

    for base in [f"base-ic-pop-{year}", f"base-ic-evol-struct-pop-{year}"]:
        for ext in ["_csv.zip", ".zip", "_xlsx.zip"]:
            url = f"https://www.insee.fr/fr/statistiques/fichier/{pid}/{base}{ext}"
            zp = DATA / f"iris_{year}.zip"
            try:
                r = requests.get(url, timeout=120)
                if r.status_code == 200 and len(r.content) > 10000:
                    zp.write_bytes(r.content)
                    with zipfile.ZipFile(zp) as zf:
                        best = sorted([n for n in zf.namelist() if n.endswith((".csv", ".xls", ".xlsx"))],
                                      key=lambda n: zf.getinfo(n).file_size, reverse=True)
                        if best:
                            zf.extract(best[0], DATA)
                            out = DATA / best[0]
                            print(f"  [ok] {year}: extrait {best[0]}")
                            return out
            except:
                continue
    return None


# ==========================================================================
# IRIS DATA LOADING
# ==========================================================================
def read_tabular(path):
    """Read CSV or XLS/XLSX with auto-detection."""
    ext = path.suffix.lower()
    if ext == ".csv":
        for sep in [";", ",", "\t"]:
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, low_memory=False)
                    if len(df.columns) > 5:
                        return df
                except:
                    continue
    else:
        for skip in [0, 5, 6]:
            try:
                df = pd.read_excel(path, skiprows=skip, dtype=str)
                if len(df.columns) > 5:
                    return df
            except:
                continue
    return None


def col_find(df, target):
    """Case-insensitive column lookup."""
    for c in df.columns:
        if c.strip().upper() == target.upper():
            return c
    for c in df.columns:
        if target.upper().replace("_", "") in c.upper().replace("_", ""):
            return c
    return None


def load_iris(path, year, dep_codes):
    """Load IRIS data, filter by departments, compute indicators."""
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
    if iris_c: out["IRIS"] = df[iris_c].values
    if com_c:  out["COM"]  = df[com_c].astype(str).str.strip().values
    if "IRIS" in out: out["DEP"] = out["IRIS"].str[:2]
    elif "COM" in out: out["DEP"] = out["COM"].str[:2]
    if "COM" in out:
        out["ARRDT"] = out["COM"].apply(lambda x: int(x[-2:]) if x.startswith("751") else 0)

    for lc in ["LIBIRIS", "LIBCOM"]:
        fc = col_find(df, lc)
        if fc: out[lc] = df[fc].values

    vm = csp_vars(year)
    for key, var in vm.items():
        if not var: continue
        fc = col_find(df, var)
        out[key] = pd.to_numeric(df[fc], errors="coerce").fillna(0).values if fc else 0.0

    csp_keys = ["cpis", "prof_inter", "employes", "ouvriers", "retraites", "sans_act", "artisans"]
    avail = [k for k in csp_keys if k in out and out[k].sum() > 0]
    if "pop15p" not in out or out.get("pop15p", pd.Series([0])).sum() == 0:
        out["pop15p"] = out[avail].sum(axis=1) if avail else 0

    pop = out["pop15p"].replace(0, np.nan)
    out["pct_cpis"]        = (out["cpis"] / pop * 100).round(2)
    out["pct_classes_pop"] = ((out["employes"] + out["ouvriers"]) / pop * 100).round(2)
    out["pct_prof_inter"]  = (out["prof_inter"] / pop * 100).round(2)
    out["ratio_gentrif"]   = (out["pct_cpis"] / out["pct_classes_pop"].replace(0, np.nan)).round(3)

    if out.get("pop_etr", pd.Series([0])).sum() > 0:
        ptot = (out.get("pop_fr", 0) + out.get("pop_etr", 0)).replace(0, np.nan)
        out["pct_etr"] = (out["pop_etr"] / ptot * 100).round(2)

    out["year"] = year
    print(f"     CPIS={out['pct_cpis'].mean():.1f}%  Ouv+Empl={out['pct_classes_pop'].mean():.1f}%")
    return out


# ==========================================================================
# CONTOURS
# ==========================================================================
def load_iris_contours(dep_codes):
    cache = DATA / f"iris_contours_{'_'.join(dep_codes)}.geojson"
    if not cache.exists():
        filt = " OR ".join([f'dep_code="{d}"' for d in dep_codes])
        url = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-france-iris/exports/geojson?where={filt}&limit=-1"
        fetch(url, cache, f"Contours IRIS dép.{','.join(dep_codes)}")
    if not cache.exists():
        return None
    gdf = gpd.read_file(cache)
    for c in ["iris_code", "CODE_IRIS", "code_iris", "DCOMIRIS"]:
        if c in gdf.columns:
            gdf["IRIS"] = gdf[c].astype(str).str.strip()
            break
    else:
        for c in gdf.columns:
            if gdf[c].dtype == object and gdf[c].astype(str).str.match(r"^\d{9}$").sum() > 10:
                gdf["IRIS"] = gdf[c].astype(str).str.strip()
                break
    return gdf


def load_quartier_contours():
    cache = DATA / "quartiers_paris.geojson"
    if not cache.exists():
        url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/quartier_paris/exports/geojson?limit=-1"
        fetch(url, cache, "Contours 80 quartiers")
    if not cache.exists():
        return None
    gdf = gpd.read_file(cache)
    for c in gdf.columns:
        if gdf[c].dtype in [object, "int64", "float64"]:
            try:
                vals = pd.to_numeric(gdf[c], errors="coerce")
                if vals.between(1, 80).sum() >= 60:
                    gdf["num_quartier"] = vals.astype(int)
                    break
            except:
                pass
    for c in gdf.columns:
        if "c_ar" in c.lower() or "arrond" in c.lower():
            gdf["arrondissement"] = pd.to_numeric(gdf[c], errors="coerce")
            break
    return gdf


# ==========================================================================
# HISTORICAL MODULE (1982-1999, 80 quartiers)
# ==========================================================================
def quartier_template():
    """Create CSV template for manual data entry from APUR PDF."""
    rows = []
    for a, qs in QUARTIERS.items():
        for n, nom in qs:
            rows.append(dict(num_quartier=n, nom=nom, arrondissement=a))
    df = pd.DataFrame(rows)
    for y in [1982, 1990, 1999]:
        for c in ["cpis", "prof_inter", "employes", "ouvriers", "pop_totale"]:
            df[f"{c}_{y}"] = ""
    return df


def try_parse_apur_pdf():
    """Download and attempt to parse APUR PDF tables."""
    pdf_path = DATA / "apur_paris_1954_1999.pdf"
    fetch("https://www.apur.org/sites/default/files/documents/paris.pdf",
          pdf_path, "PDF APUR Paris 1954-1999")
    if not pdf_path.exists():
        return None
    try:
        import pdfplumber
    except ImportError:
        print("  [!] pip install pdfplumber pour parser le PDF")
        return None

    print("  [..] Parsing PDF APUR...")
    quartier_names = {q.lower(): n for qs in QUARTIERS.values() for n, q in qs}
    hits = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            for table in (page.extract_tables() or []):
                if not table or len(table) < 3: continue
                for row in table:
                    if not row or not row[0]: continue
                    cell = str(row[0]).strip().lower()
                    for qname, qnum in quartier_names.items():
                        if qname in cell or cell in qname:
                            vals = []
                            for v in row[1:]:
                                try: vals.append(float(str(v).replace(" ", "").replace(",", ".")))
                                except: vals.append(None)
                            hits.append(dict(num_quartier=qnum, page=i, values=vals))
                            break
    print(f"  -> {len(hits)} lignes quartier extraites")
    return hits if hits else None


def load_historical():
    """Load 1982-1999 quartier data from CSV or generate template."""
    for candidate in [DATA / "quartiers_csp_data.csv", DATA / "quartiers_csp_template.csv"]:
        if candidate.exists():
            df = pd.read_csv(candidate, sep=";", encoding="utf-8")
            results = {}
            for y in [1982, 1990, 1999]:
                cc = f"cpis_{y}"
                if cc not in df.columns: continue
                vals = pd.to_numeric(df[cc], errors="coerce")
                if vals.isna().all() or vals.sum() == 0: continue
                ydf = df[["num_quartier", "nom", "arrondissement"]].copy()
                for k in ["cpis", "prof_inter", "employes", "ouvriers", "pop_totale"]:
                    ydf[k] = pd.to_numeric(df.get(f"{k}_{y}", 0), errors="coerce").fillna(0)
                pop = ydf["pop_totale"].replace(0, np.nan)
                ydf["pct_cpis"] = (ydf["cpis"] / pop * 100).round(2)
                ydf["pct_classes_pop"] = ((ydf["employes"] + ydf["ouvriers"]) / pop * 100).round(2)
                ydf["ratio_gentrif"] = (ydf["pct_cpis"] / ydf["pct_classes_pop"].replace(0, np.nan)).round(3)
                ydf["year"] = y
                results[y] = ydf
                print(f"  -> {y}: {len(ydf)} quartiers CPIS={ydf['pct_cpis'].mean():.1f}%")
            if results:
                return results

    try_parse_apur_pdf()

    tpl = quartier_template()
    tpl_path = DATA / "quartiers_csp_template.csv"
    tpl.to_csv(tpl_path, index=False, sep=";")
    print(f"\n  [tpl] Template créé : {tpl_path}")
    print(f"        Remplis avec le Tableau 3 du PDF APUR")
    print(f"        Sauvegarde sous : {DATA}/quartiers_csp_data.csv")
    print(f"        Puis relance le script\n")
    return {}


# ==========================================================================
# MAPPING
# ==========================================================================
def plot_map(gdf, col, title, path, cmap="Blues", diverging=False):
    fig, ax = plt.subplots(1, 1, figsize=(14, 16))
    fig.patch.set_facecolor("white"); ax.set_facecolor("#f5f5f5")
    d = gdf[gdf[col].notna() & np.isfinite(gdf[col])]
    if len(d) == 0: plt.close(); return

    if diverging:
        mx = max(abs(d[col].quantile(0.02)), abs(d[col].quantile(0.98)))
        d.plot(column=col, ax=ax, cmap="RdBu_r", vmin=-mx, vmax=mx,
               legend=True, edgecolor="white", linewidth=0.1,
               legend_kwds=dict(label="Δ pts %", shrink=0.5, orientation="horizontal", pad=0.02))
    else:
        try:
            import mapclassify
            d.plot(column=col, ax=ax, cmap=cmap, scheme="quantiles", k=7,
                   legend=True, edgecolor="white", linewidth=0.1,
                   legend_kwds=dict(loc="lower left", fontsize=7, title="%"))
        except:
            d.plot(column=col, ax=ax, cmap=cmap,
                   vmin=d[col].quantile(0.02), vmax=d[col].quantile(0.98),
                   legend=True, edgecolor="white", linewidth=0.1,
                   legend_kwds=dict(label="%", shrink=0.5, orientation="horizontal", pad=0.02))

    if "COM" in d.columns:
        try:
            c = d.dissolve(by="COM")
            c.boundary.plot(ax=ax, edgecolor="black", linewidth=0.6, alpha=0.7)
        except: pass
    ax.set_axis_off()
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    fig.text(0.5, 0.02, "Source: INSEE | D'après Clerval (2010)", ha="center", fontsize=7, color="gray")
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white"); plt.close()
    print(f"    [map] {path.name}")


def plot_multitemp(gdfs, col, title, path, cmap="Blues"):
    years = sorted(gdfs.keys())
    n = len(years)
    if n == 0: return
    fig, axes = plt.subplots(1, n, figsize=(7 * n, 9))
    fig.patch.set_facecolor("white")
    if n == 1: axes = [axes]

    vals = pd.concat([g[col].dropna() for g in gdfs.values()])
    vmin, vmax = vals.quantile(0.02), vals.quantile(0.98)

    for ax, y in zip(axes, years):
        ax.set_facecolor("#f5f5f5")
        d = gdfs[y][gdfs[y][col].notna()]
        d.plot(column=col, ax=ax, cmap=cmap, vmin=vmin, vmax=vmax,
               edgecolor="white", linewidth=0.08)
        if "COM" in d.columns:
            try: d.dissolve(by="COM").boundary.plot(ax=ax, edgecolor="black", linewidth=0.4, alpha=0.6)
            except: pass
        ax.set_axis_off(); ax.set_title(str(y), fontsize=12, fontweight="bold")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin, vmax)); sm.set_array([])
    fig.colorbar(sm, ax=axes, orientation="horizontal", fraction=0.03, pad=0.05, shrink=0.6).set_label("%")
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.98)
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white"); plt.close()
    print(f"    [map] {path.name}")


def plot_synthesis(gdf, label, path):
    fig, ax = plt.subplots(1, 1, figsize=(16, 18))
    fig.patch.set_facecolor("white"); ax.set_facecolor("#f0f0f0")
    d = gdf[gdf["ratio_gentrif"].notna()].copy()
    if len(d) == 0: plt.close(); return

    r = d["ratio_gentrif"]
    cats = [
        ("Beaux quartiers",        r.quantile(0.95), np.inf,           "#0c2340"),
        ("Gentrification achevée", r.quantile(0.80), r.quantile(0.95), "#1a5276"),
        ("Gentrif. avancée",       r.quantile(0.60), r.quantile(0.80), "#2e86c1"),
        ("Gentrif. en cours",      r.quantile(0.40), r.quantile(0.60), "#85c1e9"),
        ("En transition",          r.quantile(0.20), r.quantile(0.40), "#d5f5e3"),
        ("Quartiers populaires",   0,                r.quantile(0.20), "#e74c3c"),
    ]
    for lbl, lo, hi, col in cats:
        sub = d[(d["ratio_gentrif"] >= lo) & (d["ratio_gentrif"] < hi)]
        if len(sub): sub.plot(ax=ax, color=col, edgecolor="white", linewidth=0.1)

    if "COM" in d.columns:
        try:
            cm = d.dissolve(by="COM"); cm.boundary.plot(ax=ax, edgecolor="black", linewidth=0.8)
            for idx, row in cm.iterrows():
                c = row.geometry.centroid; s = str(idx)
                if s.startswith("751"):
                    ax.annotate(f"{s[-2:].lstrip('0')}e", xy=(c.x, c.y), ha="center", va="center",
                                fontsize=6, fontweight="bold", color="white",
                                path_effects=[pe.withStroke(linewidth=2, foreground="black")])
        except: pass

    ax.legend(handles=[Patch(facecolor=c, edgecolor="gray", label=l) for l, _, _, c in cats],
              loc="lower left", fontsize=8, title="Classification")
    ax.set_axis_off()
    y = d["year"].iloc[0] if "year" in d else "?"
    ax.set_title(f"Synthèse gentrification — {label} ({y})", fontsize=13, fontweight="bold", pad=15)
    plt.savefig(path, dpi=200, bbox_inches="tight", facecolor="white"); plt.close()
    print(f"    [map] {path.name}")


def plot_historical_maps(qdata, contours):
    """Maps for 1982-1999 at quartier level."""
    years = sorted(qdata.keys())
    if not years or contours is None: return

    for indicator, title_base, cmap, fname in [
        ("pct_classes_pop", "Ouvriers + employés", "Reds", "hist_classes_pop"),
        ("pct_cpis", "CPIS", "Blues", "hist_cpis"),
    ]:
        n = len(years)
        fig, axes = plt.subplots(1, n, figsize=(7 * n, 9))
        fig.patch.set_facecolor("white")
        if n == 1: axes = [axes]

        merged_all = [contours.merge(qdata[y], on="num_quartier") for y in years]
        all_v = pd.concat([m[indicator].dropna() for m in merged_all if indicator in m])
        vmin, vmax = (all_v.quantile(0.02), all_v.quantile(0.98)) if len(all_v) else (0, 100)

        for ax, y, mg in zip(axes, years, merged_all):
            ax.set_facecolor("#f5f5f5")
            if indicator in mg and not mg[indicator].isna().all():
                mg.plot(column=indicator, ax=ax, cmap=cmap, vmin=vmin, vmax=vmax,
                        edgecolor="black", linewidth=0.3, missing_kwds=dict(color="lightgray"))
                arr_c = "arrondissement_x" if "arrondissement_x" in mg else "arrondissement"
                if arr_c in mg.columns:
                    try: mg.dissolve(by=arr_c).boundary.plot(ax=ax, edgecolor="black", linewidth=1.2)
                    except: pass
                ax.text(0.98, 0.02, f"Moy: {mg[indicator].mean():.1f}%",
                        transform=ax.transAxes, ha="right", va="bottom", fontsize=9,
                        bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))
            ax.set_axis_off(); ax.set_title(str(y), fontsize=13, fontweight="bold")

        sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin, vmax)); sm.set_array([])
        fig.colorbar(sm, ax=axes, orientation="horizontal", fraction=0.03, pad=0.05, shrink=0.5).set_label("%")
        fig.suptitle(f"{title_base} — 80 quartiers de Paris", fontsize=14, fontweight="bold", y=0.98)
        fig.text(0.5, 0.01, "Source: INSEE RGP | APUR | D'après Clerval (2010)",
                 ha="center", fontsize=7, color="gray")
        p = OUT / f"{fname}_{'_'.join(map(str, years))}.png"
        plt.savefig(p, dpi=180, bbox_inches="tight", facecolor="white"); plt.close()
        print(f"    [map] {p.name}")


# ==========================================================================
# MAIN
# ==========================================================================
def run_historical():
    print("\n" + "=" * 60)
    print("  HISTORIQUE — 1982, 1990, 1999 (80 quartiers)")
    print("=" * 60)
    qdata = load_historical()
    contours = load_quartier_contours()
    if qdata and contours is not None:
        plot_historical_maps(qdata, contours)
        for y, df in qdata.items():
            p = OUT / f"data_quartiers_{y}.csv"
            df.to_csv(p, index=False, sep=";"); print(f"  [csv] {p.name}")
    return qdata


def run_iris():
    print("\n" + "=" * 60)
    print("  IRIS — 2007, 2012, 2017, 2022 (Grand Paris)")
    print("=" * 60)

    YEARS = [2007, 2012, 2017, 2022]
    raw = {}
    for y in YEARS:
        p = find_data_file(y)
        if p: raw[y] = p

    if not raw:
        print("  [!] Aucune donnée. Télécharge depuis INSEE (cf CLAUDE.md)")
        return {}

    datasets = {}
    for y, p in raw.items():
        df = load_iris(p, y, DEPS_GP)
        if df is not None and len(df) > 0:
            datasets[y] = df

    for scope, (deps, label) in SCOPES.items():
        print(f"\n  >> {label}")
        contours = load_iris_contours(deps)
        if contours is None or "IRIS" not in contours.columns:
            print(f"    [!] Pas de contours — skip"); continue

        scope_data = {y: df[df["DEP"].isin(deps)] for y, df in datasets.items()}
        scope_data = {y: d for y, d in scope_data.items() if len(d) > 0}
        if not scope_data: continue

        gdfs = {}
        for y, df in scope_data.items():
            mg = contours.merge(df, on="IRIS", how="inner")
            if len(mg) == 0: continue
            gdfs[y] = mg
            plot_map(mg, "pct_cpis", f"CPIS — {label} ({y})",
                     OUT / f"cpis_{scope}_{y}.png", cmap="Blues")
            plot_map(mg, "pct_classes_pop", f"Ouv.+Empl. — {label} ({y})",
                     OUT / f"classes_pop_{scope}_{y}.png", cmap="Reds")

        if len(gdfs) >= 2:
            plot_multitemp(gdfs, "pct_cpis", f"CPIS — {label}",
                           OUT / f"multitemp_cpis_{scope}.png", "Blues")
            plot_multitemp(gdfs, "pct_classes_pop", f"Ouv.+Empl. — {label}",
                           OUT / f"multitemp_cp_{scope}.png", "Reds")

            ys = sorted(gdfs); first, last = ys[0], ys[-1]
            ev = gdfs[last][["IRIS", "geometry", "COM", "pct_cpis", "pct_classes_pop"]].copy()
            ev.columns = ["IRIS", "geometry", "COM", "cpis_l", "cp_l"]
            ev = ev.merge(gdfs[first][["IRIS", "pct_cpis", "pct_classes_pop"]].rename(
                columns={"pct_cpis": "cpis_f", "pct_classes_pop": "cp_f"}), on="IRIS", how="inner")
            ev["evol_cpis"] = ev["cpis_l"] - ev["cpis_f"]
            ev["evol_cp"] = ev["cp_l"] - ev["cp_f"]
            eg = gpd.GeoDataFrame(ev, geometry="geometry")
            if len(eg):
                plot_map(eg, "evol_cpis", f"Δ CPIS {first}→{last} — {label}",
                         OUT / f"evol_cpis_{scope}_{first}_{last}.png", diverging=True)
                plot_map(eg, "evol_cp", f"Δ Ouv+Empl {first}→{last} — {label}",
                         OUT / f"evol_cp_{scope}_{first}_{last}.png", diverging=True)

        if gdfs:
            ly = max(gdfs)
            if "pct_etr" in gdfs[ly] and gdfs[ly]["pct_etr"].sum() > 0:
                plot_map(gdfs[ly], "pct_etr", f"Pop. étrangère — {label} ({ly})",
                         OUT / f"pop_etr_{scope}_{ly}.png", cmap="YlOrRd")
            plot_synthesis(gdfs[ly], label, OUT / f"synthese_{scope}_{ly}.png")

    for y, df in datasets.items():
        cols = [c for c in ["IRIS", "COM", "DEP", "ARRDT", "pct_cpis", "pct_classes_pop",
                            "ratio_gentrif", "pct_etr", "year"] if c in df.columns]
        p = OUT / f"data_iris_{y}.csv"
        df[cols].to_csv(p, index=False, sep=";"); print(f"  [csv] {p.name}")

    return datasets


def main():
    parser = argparse.ArgumentParser(description="Gentrification Paris & Grand Paris")
    parser.add_argument("--historical", action="store_true", help="Module historique seul")
    parser.add_argument("--iris", action="store_true", help="Module IRIS seul")
    args = parser.parse_args()

    print("=" * 60)
    print("  GENTRIFICATION PARIS & GRAND PARIS")
    print("  D'après Clerval (2010), Cybergeo n°505")
    print("=" * 60)

    run_all = not args.historical and not args.iris

    if args.historical or run_all:
        run_historical()
    if args.iris or run_all:
        run_iris()

    if OUT.exists():
        files = sorted(OUT.iterdir())
        maps = [f for f in files if f.suffix == ".png"]
        csvs = [f for f in files if f.suffix == ".csv"]
        total = sum(f.stat().st_size for f in files) / 1024 ** 2
        print(f"\n{'=' * 60}")
        print(f"  {len(files)} fichiers ({total:.1f} Mo) dans output/")
        print(f"     {len(maps)} cartes | {len(csvs)} CSV")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
