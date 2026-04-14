#!/usr/bin/env python3
"""
Étape 3 du pipeline — Produit les cartes et tables dans output/.

Consomme les parquet long de data/processed et les contours de data/raw.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from gentrif.config import (
    DATA_INTERIM,
    DATA_PROCESSED,
    OUT_FIGURES,
    OUT_TABLES,
    SCOPES,
)

# Cartes de synthèse (grand_paris, indicateurs structurants) → key_maps/.
# Tout le reste (Paris-seul, petite-couronne, indicateurs symétriques ou
# de triangulation) → annexes/. Le routage est purement conventionnel et
# fondé sur le nom de fichier pour rester transparent.
_KEY_PREFIXES: tuple[str, ...] = (
    "trajectoire_grand_paris",
    "long_trajectoire_grand_paris",
    "multitemp_cpis_grand_paris",
    "long_multitemp_cpis_grand_paris",
    "niveau_grand_paris",
    "evol_cpis_grand_paris",
)


def _fig(name: str) -> Path:
    sub = "key_maps" if name.startswith(_KEY_PREFIXES) else "annexes"
    d = OUT_FIGURES / sub
    d.mkdir(parents=True, exist_ok=True)
    return d / name
from gentrif.loaders import (
    load_commune_contours_gdf,
    load_historical_quartiers,
    load_iris_contours_gdf,
    load_quartier_contours_gdf,
)
from gentrif.viz import (
    plot_historical_maps,
    plot_level_typology,
    plot_map,
    plot_multitemp,
    plot_trajectory,
)


def _wide_by_year(prefix: str = "iris_wide") -> dict[int, pd.DataFrame]:
    """Rassemble les DataFrames wide produits en interim par build_processed."""
    out = {}
    for p in sorted(DATA_INTERIM.glob(f"{prefix}_*.parquet")):
        year = int(p.stem.split("_")[-1])
        out[year] = pd.read_parquet(p)
    return out


def run_iris_maps() -> None:
    print("\n-- IRIS (2007-2022)")
    wide = _wide_by_year()
    if not wide:
        print("  [!] Aucun DataFrame en interim. Lance d'abord "
              "scripts/build_processed.py")
        return

    for scope, (deps, label) in SCOPES.items():
        print(f"\n  >> {label}")
        contours = load_iris_contours_gdf(deps)
        if contours is None or "IRIS" not in contours.columns:
            print(f"    [!] Pas de contours — skip")
            continue

        scope_data = {y: df[df["DEP"].isin(deps)] for y, df in wide.items()}
        scope_data = {y: d for y, d in scope_data.items() if len(d) > 0}
        if not scope_data:
            continue

        gdfs: dict[int, gpd.GeoDataFrame] = {}
        for y, df in scope_data.items():
            mg = contours.merge(df, on="IRIS", how="inner")
            if len(mg) == 0:
                continue
            gdfs[y] = mg

        if len(gdfs) >= 2:
            plot_multitemp(gdfs, "pct_cpis", f"CPIS — {label}",
                           _fig(f"multitemp_cpis_{scope}.png"), "Blues")
            plot_multitemp(gdfs, "pct_classes_pop",
                           f"Ouv.+Empl. — {label}",
                           _fig(f"multitemp_cp_{scope}.png"), "Reds")

            ys = sorted(gdfs)
            first, last = ys[0], ys[-1]
            ev = gdfs[last][["IRIS", "geometry", "COM",
                             "pct_cpis", "pct_classes_pop"]].copy()
            ev.columns = ["IRIS", "geometry", "COM", "cpis_l", "cp_l"]
            ev = ev.merge(
                gdfs[first][["IRIS", "pct_cpis", "pct_classes_pop"]]
                .rename(columns={"pct_cpis": "cpis_f",
                                 "pct_classes_pop": "cp_f"}),
                on="IRIS", how="inner",
            )
            ev["evol_cpis"] = ev["cpis_l"] - ev["cpis_f"]
            ev["evol_cp"] = ev["cp_l"] - ev["cp_f"]
            eg = gpd.GeoDataFrame(ev, geometry="geometry")
            if len(eg):
                plot_map(eg, "evol_cpis",
                         f"Δ CPIS {first}→{last} — {label}",
                         _fig(f"evol_cpis_{scope}_{first}_{last}.png"),
                         diverging=True)
                plot_map(eg, "evol_cp",
                         f"Δ Ouv+Empl {first}→{last} — {label}",
                         _fig(f"evol_cp_{scope}_{first}_{last}.png"),
                         diverging=True)

        if gdfs:
            ly = max(gdfs)
            if "pct_etr" in gdfs[ly] and gdfs[ly]["pct_etr"].sum() > 0:
                plot_map(gdfs[ly], "pct_etr",
                         f"Pop. étrangère — {label} ({ly})",
                         _fig(f"pop_etr_{scope}_{ly}.png"), cmap="YlOrRd")
            plot_level_typology(
                gdfs[ly], label,
                _fig(f"niveau_{scope}_{ly}.png"),
            )
            if len(gdfs) >= 2:
                fy = min(gdfs)
                plot_trajectory(
                    gdfs[fy], gdfs[ly], label,
                    _fig(f"trajectoire_{scope}_{fy}_{ly}.png"),
                )


def run_filosofi_maps() -> None:
    """Trajectoire des revenus (FiLoSoFi) — triangulation avec la CSP.

    Utilise `rel_med_uc` (revenu médian UC / médiane du périmètre) comme
    équivalent de `ratio_gentrif` dans le classifieur trajectoire. Un IRIS
    dont le revenu relatif passe d'un niveau bas à une hausse est en cours
    de gentrification au sens économique.
    """
    print("\n-- FiLoSoFi IRIS — trajectoires revenus")
    wide = _wide_by_year("filosofi_wide")
    if len(wide) < 2:
        print("  [!] FiLoSoFi indispo ou un seul millésime — skip")
        return

    for scope, (deps, label) in SCOPES.items():
        print(f"\n  >> {label}")
        contours = load_iris_contours_gdf(deps)
        if contours is None or "IRIS" not in contours.columns:
            continue

        scope_data = {y: df[df["DEP"].isin(deps)].copy()
                      for y, df in wide.items()}
        scope_data = {y: d.rename(columns={"rel_med_uc": "ratio_gentrif"})
                      for y, d in scope_data.items() if len(d) > 0}
        if len(scope_data) < 2:
            continue

        gdfs: dict[int, gpd.GeoDataFrame] = {}
        for y, df in scope_data.items():
            mg = contours.merge(df, on="IRIS", how="inner")
            if len(mg):
                gdfs[y] = mg

        if len(gdfs) < 2:
            continue
        fy, ly = min(gdfs), max(gdfs)
        from gentrif.viz import plot_trajectory as _pt
        _pt(gdfs[fy], gdfs[ly], f"{label} (revenus)",
            _fig(f"trajectoire_revenus_{scope}_{fy}_{ly}.png"))


def run_long_series_maps() -> None:
    """Tendance longue 1968-2022 au niveau commune (Grand Paris).

    Produit une carte multi-temporelle et la trajectoire 2×2 entre le
    premier et le dernier millésime disponibles.
    """
    print("\n-- Tendance longue 1968-2022 (communes)")
    src = DATA_INTERIM / "long_series_wide.parquet"
    if not src.exists():
        print("  [!] long_series_wide.parquet absent — skip")
        return

    wide = pd.read_parquet(src)
    if wide.empty:
        return

    for scope, (deps, label) in SCOPES.items():
        print(f"\n  >> {label}")
        contours = load_commune_contours_gdf(deps)
        if contours is None or "CODGEO" not in contours.columns:
            print("    [!] contours communes indispos — skip")
            continue

        scope_df = wide[wide["DEP"].isin(deps)].copy()
        if len(scope_df) == 0:
            continue

        gdfs: dict[int, gpd.GeoDataFrame] = {}
        for y, sub in scope_df.groupby("year"):
            mg = contours.merge(sub, on="CODGEO", how="inner")
            if len(mg):
                gdfs[int(y)] = mg

        if len(gdfs) >= 2:
            plot_multitemp(gdfs, "pct_cpis",
                           f"CPIS (actifs) — {label}",
                           _fig(f"long_multitemp_cpis_{scope}.png"),
                           "Blues")
            fy, ly = min(gdfs), max(gdfs)
            from gentrif.viz import plot_trajectory as _pt
            _pt(gdfs[fy], gdfs[ly], f"{label} (tendance longue)",
                _fig(f"long_trajectoire_{scope}_{fy}_{ly}.png"),
                key="CODGEO")


def run_historical_maps() -> None:
    print("\n-- Historique 1982-1999 (80 quartiers)")
    qdata = load_historical_quartiers()
    contours = load_quartier_contours_gdf()
    if qdata and contours is not None:
        plot_historical_maps(qdata, contours, OUT_FIGURES)
        for y, df in qdata.items():
            p = OUT_TABLES / f"quartiers_{y}.csv"
            df.to_csv(p, index=False, sep=";")
            print(f"  [csv] {p.name}")


def main() -> None:
    print("=" * 60)
    print("  [3/3] MAPS — génération des figures et tables")
    print("=" * 60)
    run_historical_maps()
    run_long_series_maps()
    run_iris_maps()
    run_filosofi_maps()
    print("\n[done]")


if __name__ == "__main__":
    main()
