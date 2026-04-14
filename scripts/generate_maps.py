#!/usr/bin/env python3
"""
Étape 3 du pipeline — Produit les cartes et tables dans output/.

Consomme les parquet long de data/processed et les contours de data/raw.
"""
from __future__ import annotations

import geopandas as gpd
import pandas as pd

from gentrif.config import (
    DATA_INTERIM,
    DATA_PROCESSED,
    OUT_FIGURES,
    OUT_TABLES,
    SCOPES,
)
from gentrif.loaders import (
    load_historical_quartiers,
    load_iris_contours_gdf,
    load_quartier_contours_gdf,
)
from gentrif.viz import (
    plot_historical_maps,
    plot_map,
    plot_multitemp,
    plot_synthesis,
)


def _wide_by_year() -> dict[int, pd.DataFrame]:
    """Rassemble les DataFrames wide produits en interim par build_processed."""
    out = {}
    for p in sorted(DATA_INTERIM.glob("iris_wide_*.parquet")):
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
            plot_map(mg, "pct_cpis", f"CPIS — {label} ({y})",
                     OUT_FIGURES / f"cpis_{scope}_{y}.png", cmap="Blues")
            plot_map(mg, "pct_classes_pop",
                     f"Ouv.+Empl. — {label} ({y})",
                     OUT_FIGURES / f"classes_pop_{scope}_{y}.png", cmap="Reds")

        if len(gdfs) >= 2:
            plot_multitemp(gdfs, "pct_cpis", f"CPIS — {label}",
                           OUT_FIGURES / f"multitemp_cpis_{scope}.png", "Blues")
            plot_multitemp(gdfs, "pct_classes_pop",
                           f"Ouv.+Empl. — {label}",
                           OUT_FIGURES / f"multitemp_cp_{scope}.png", "Reds")

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
                         OUT_FIGURES / f"evol_cpis_{scope}_{first}_{last}.png",
                         diverging=True)
                plot_map(eg, "evol_cp",
                         f"Δ Ouv+Empl {first}→{last} — {label}",
                         OUT_FIGURES / f"evol_cp_{scope}_{first}_{last}.png",
                         diverging=True)

        if gdfs:
            ly = max(gdfs)
            if "pct_etr" in gdfs[ly] and gdfs[ly]["pct_etr"].sum() > 0:
                plot_map(gdfs[ly], "pct_etr",
                         f"Pop. étrangère — {label} ({ly})",
                         OUT_FIGURES / f"pop_etr_{scope}_{ly}.png", cmap="YlOrRd")
            plot_synthesis(gdfs[ly], label,
                           OUT_FIGURES / f"synthese_{scope}_{ly}.png")


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
    run_iris_maps()
    print("\n[done]")


if __name__ == "__main__":
    main()
