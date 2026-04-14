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
    FILOSOFI_YEARS,
    IRIS_YEARS,
)
from gentrif.fetch import (
    fetch_filosofi_year,
    fetch_iris_year,
    fetch_long_series,
)
from gentrif.harmonize import (
    apply_crosswalk_wide,
    harmonize_iris,
    load_iris_crosswalk,
    to_long,
)
from gentrif.loaders import (
    load_filosofi_iris,
    load_historical_quartiers,
    load_iris,
    load_long_series,
)


def build_iris_long() -> pd.DataFrame:
    rows = []
    crosswalk = load_iris_crosswalk()
    if crosswalk is not None:
        print(f"  [ok] crosswalk IRIS chargé : {len(crosswalk):,} mappings")

    for year in IRIS_YEARS:
        path = fetch_iris_year(year)
        if path is None:
            print(f"  [!] {year}: aucune donnée")
            continue
        df = load_iris(path, year, DEPS_GRAND_PARIS)
        if df is None or df.empty:
            continue

        if crosswalk is not None:
            df = apply_crosswalk_wide(df, crosswalk, iris_col="IRIS")
            print(f"     crosswalk appliqué -> {len(df):,} IRIS harmonisés")

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


def build_filosofi_long() -> pd.DataFrame:
    rows = []
    for year in FILOSOFI_YEARS:
        path = fetch_filosofi_year(year)
        if path is None:
            print(f"  [!] FiLoSoFi {year}: aucune donnée")
            continue
        df = load_filosofi_iris(path, year, DEPS_GRAND_PARIS)
        if df is None or df.empty:
            continue

        wide = DATA_INTERIM / f"filosofi_wide_{year}.parquet"
        df.to_parquet(wide, index=False)
        print(f"  [interim] {wide.name}")

        long = to_long(df, geo_level="iris_filosofi",
                       geo_code_col="IRIS", geo_name_col=None)
        rows.append(long)

    if not rows:
        return pd.DataFrame()
    full = pd.concat(rows, ignore_index=True)
    out = DATA_PROCESSED / "filosofi_long.parquet"
    full.to_parquet(out, index=False)
    print(f"  [processed] {out.name} — {len(full):,} lignes")
    return full


def build_long_series_long() -> pd.DataFrame:
    """Séries harmonisées INSEE 1968-2022 au niveau commune."""
    path = fetch_long_series()
    if path is None:
        print("  [!] Séries longues introuvables — skip")
        return pd.DataFrame()

    df = load_long_series(path, DEPS_GRAND_PARIS)
    if df.empty:
        print("  [!] Aucune donnée CSP exploitable dans les séries longues")
        return pd.DataFrame()

    wide = DATA_INTERIM / "long_series_wide.parquet"
    df.to_parquet(wide, index=False)
    print(f"  [interim] {wide.name} ({len(df):,} lignes)")

    long = to_long(df, geo_level="commune",
                   geo_code_col="CODGEO", geo_name_col="LIBGEO")
    out = DATA_PROCESSED / "long_series_long.parquet"
    long.to_parquet(out, index=False)
    print(f"  [processed] {out.name} — {len(long):,} lignes")
    return long


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
    print("\n-- FiLoSoFi IRIS (revenus)")
    build_filosofi_long()
    print("\n-- Séries longues 1968-2022 (communes)")
    build_long_series_long()
    print("\n-- Quartiers APUR 1982-1999")
    build_quartiers_long()
    print("\n[done]")


if __name__ == "__main__":
    main()
