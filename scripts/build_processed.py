#!/usr/bin/env python3
"""
Étape 2 du pipeline — Construit les tables analysis-ready à partir
des sources brutes, en format long canonique et en parquet.

Sorties :
    data/processed/iris_long.parquet       — IRIS × années, format tidy
    data/processed/quartiers_long.parquet  — 80 quartiers × années APUR
    data/interim/iris_wide_{year}.parquet  — DataFrames wide par millésime

Schéma long canonique :
    year, geo_level, geo_code, geo_name, indicator, value
"""
from __future__ import annotations

import pandas as pd

from gentrif.config import (
    DATA_INTERIM,
    DATA_PROCESSED,
    DEPS_GRAND_PARIS,
    IRIS_YEARS,
)
from gentrif.fetch import fetch_iris_year
from gentrif.harmonize import harmonize_iris, to_long
from gentrif.loaders import load_historical_quartiers, load_iris


def build_iris_long() -> pd.DataFrame:
    rows = []
    for year in IRIS_YEARS:
        path = fetch_iris_year(year)
        if path is None:
            print(f"  [!] {year}: aucune donnée")
            continue
        df = load_iris(path, year, DEPS_GRAND_PARIS)
        if df is None or df.empty:
            continue

        # Sauvegarde wide en interim pour inspection
        wide_path = DATA_INTERIM / f"iris_wide_{year}.parquet"
        df.to_parquet(wide_path, index=False)
        print(f"  [interim] {wide_path.name}")

        name_col = "LIBIRIS" if "LIBIRIS" in df.columns else None
        long = to_long(df, geo_level="iris",
                       geo_code_col="IRIS", geo_name_col=name_col)
        rows.append(long)

    if not rows:
        return pd.DataFrame()

    full = pd.concat(rows, ignore_index=True)
    full = harmonize_iris(full)
    out = DATA_PROCESSED / "iris_long.parquet"
    full.to_parquet(out, index=False)
    print(f"  [processed] {out.name} — {len(full):,} lignes")
    return full


def build_quartiers_long() -> pd.DataFrame:
    qdata = load_historical_quartiers()
    if not qdata:
        print("  [!] Pas de données APUR quartier — template à remplir "
              "(cf. data/raw/quartiers_csp_template.csv)")
        return pd.DataFrame()

    longs = []
    for year, df in qdata.items():
        long = to_long(df, geo_level="quartier",
                       geo_code_col="num_quartier", geo_name_col="nom")
        longs.append(long)

    full = pd.concat(longs, ignore_index=True)
    out = DATA_PROCESSED / "quartiers_long.parquet"
    full.to_parquet(out, index=False)
    print(f"  [processed] {out.name} — {len(full):,} lignes")
    return full


def main() -> None:
    print("=" * 60)
    print("  [2/3] BUILD — data/raw -> data/processed (parquet long)")
    print("=" * 60)
    print("\n-- IRIS 2007-2022")
    build_iris_long()
    print("\n-- Quartiers APUR 1982-1999")
    build_quartiers_long()
    print("\n[done]")


if __name__ == "__main__":
    main()
