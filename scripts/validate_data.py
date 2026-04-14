#!/usr/bin/env python3
"""
Étape 4 (optionnelle) — Sanity checks sur la cohérence des données
produites par le pipeline.

Vérifie :
  1. Cohérence population : somme des CSP ≈ pop de référence par IRIS
  2. Plages de valeurs : ratio_gentrif, parts, revenus dans des bornes
     plausibles
  3. Continuité temporelle : pas de saut > ×2 ou < /2 entre deux
     recensements consécutifs
  4. Benchmarks agrégés : comparaison avec chiffres INSEE publiés
  5. Équilibre du crosswalk : somme des poids par IRIS source ≈ 1
  6. Distribution trajectoire : pas de classe dominante à >90%
  7. Corrélation CSP × revenus : sanity de la triangulation

Produit un rapport texte en stdout + tableau CSV dans output/tables/.
Code de retour non-zéro si anomalies critiques.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from gentrif.config import (
    DATA_INTERIM,
    DATA_PROCESSED,
    DATA_RAW,
    DEPS_GRAND_PARIS,
    IRIS_YEARS,
    LONG_SERIES_YEARS,
    OUT_TABLES,
)
from gentrif.harmonize import load_iris_crosswalk


# =============================================================================
# Helpers de rapport
# =============================================================================

CHECKS: list[dict] = []


def report(name: str, status: str, detail: str = "") -> None:
    """Stocke un résultat de check et l'affiche."""
    icon = {"ok": "✓", "warn": "⚠", "fail": "✗"}.get(status, "·")
    print(f"  [{icon}] {name}: {detail}")
    CHECKS.append(dict(check=name, status=status, detail=detail))


# =============================================================================
# Chargement des données
# =============================================================================

def _load_iris_wide() -> dict[int, pd.DataFrame]:
    return {int(p.stem.split("_")[-1]): pd.read_parquet(p)
            for p in sorted(DATA_INTERIM.glob("iris_wide_*.parquet"))}


def _load_filosofi_wide() -> dict[int, pd.DataFrame]:
    return {int(p.stem.split("_")[-1]): pd.read_parquet(p)
            for p in sorted(DATA_INTERIM.glob("filosofi_wide_*.parquet"))}


def _load_long_series_wide() -> pd.DataFrame:
    p = DATA_INTERIM / "long_series_wide.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


# =============================================================================
# Check 1 — Cohérence population (somme des CSP)
# =============================================================================

def check_population_coherence(wide: dict[int, pd.DataFrame]) -> None:
    """Pour chaque IRIS : somme des 7 CSP doit être ≈ pop15p (±15%)."""
    print("\n[1] Cohérence population par IRIS (Σ CSP vs pop15p)")
    CSP_COLS = ["cpis", "prof_inter", "employes", "ouvriers",
                "retraites", "sans_act", "artisans"]
    for year, df in wide.items():
        if "pop15p" not in df.columns:
            report(f"pop {year}", "warn", "pop15p absent")
            continue
        d = df[df["pop15p"] > 200].copy()   # exclure petits IRIS
        csp_sum = d[[c for c in CSP_COLS if c in d.columns]].sum(axis=1)
        ratio = csp_sum / d["pop15p"]
        pct_out = ((ratio < 0.85) | (ratio > 1.15)).mean() * 100
        median = ratio.median()
        status = "ok" if pct_out < 5 else ("warn" if pct_out < 20 else "fail")
        report(f"pop {year}", status,
               f"{pct_out:.1f}% IRIS hors [0.85-1.15] (médiane Σ/pop={median:.3f})")


# =============================================================================
# Check 2 — Plages de valeurs plausibles
# =============================================================================

def check_value_ranges(wide: dict[int, pd.DataFrame],
                       filo: dict[int, pd.DataFrame]) -> None:
    print("\n[2] Plages de valeurs plausibles")
    for year, df in wide.items():
        for col, lo, hi in [
            ("pct_cpis", 0, 100),
            ("pct_classes_pop", 0, 100),
            ("ratio_gentrif", 0, 20),
        ]:
            if col not in df:
                continue
            v = df[col].dropna()
            out = ((v < lo) | (v > hi)).sum()
            status = "ok" if out == 0 else "warn"
            report(f"{col} {year} ∈ [{lo},{hi}]", status,
                   f"{out} valeurs hors bornes, max={v.max():.2f}")

    for year, df in filo.items():
        if "med_uc" in df:
            v = df["med_uc"].dropna()
            lo, hi = 5_000, 100_000
            out = ((v < lo) | (v > hi)).sum()
            report(f"med_uc {year} ∈ [{lo},{hi}]€",
                   "ok" if out == 0 else "warn",
                   f"{out} IRIS hors bornes, "
                   f"médiane={v.median():.0f} min={v.min():.0f} max={v.max():.0f}")


# =============================================================================
# Check 3 — Continuité temporelle
# =============================================================================

def check_temporal_consistency(wide: dict[int, pd.DataFrame]) -> None:
    """Un IRIS stable ne devrait pas voir son ratio × 2 ou / 2 en 5 ans."""
    print("\n[3] Continuité temporelle (ratio_gentrif)")
    years = sorted(wide.keys())
    for y0, y1 in zip(years[:-1], years[1:]):
        d0 = wide[y0][["IRIS", "ratio_gentrif"]].rename(columns={"ratio_gentrif": "r0"})
        d1 = wide[y1][["IRIS", "ratio_gentrif"]].rename(columns={"ratio_gentrif": "r1"})
        m = d0.merge(d1, on="IRIS", how="inner").dropna()
        if len(m) == 0:
            continue
        ratio_change = m["r1"] / m["r0"].replace(0, np.nan)
        extreme = ((ratio_change > 2.5) | (ratio_change < 0.4)).sum()
        pct = 100 * extreme / len(m)
        status = "ok" if pct < 2 else ("warn" if pct < 10 else "fail")
        report(f"{y0}→{y1} changement x<0.4 ou x>2.5", status,
               f"{extreme} IRIS ({pct:.1f}%) sur {len(m):,}")


# =============================================================================
# Check 4 — Benchmarks agrégés
# =============================================================================

def check_aggregate_benchmarks(wide: dict[int, pd.DataFrame]) -> None:
    """Comparaison avec chiffres INSEE/Clerval bien connus."""
    print("\n[4] Benchmarks agrégés (vs chiffres de référence INSEE/Clerval)")
    if 2022 in wide:
        d = wide[2022]
        paris = d[d["DEP"] == "75"]
        pop_paris = paris["pop15p"].sum()
        cpis_pct = (paris["cpis"].sum() / pop_paris) * 100
        cp_pct = ((paris["employes"].sum() + paris["ouvriers"].sum())
                  / pop_paris) * 100
        # Benchmark INSEE Paris 2022 : CPIS+PI ≈ 55% (Paris a la plus haute
        # part de CPIS de France ; ici pop15p = actifs + inactifs + retraités)
        report("Paris intra-muros pop15p 2022",
               "ok" if 1_500_000 < pop_paris < 2_500_000 else "warn",
               f"{pop_paris/1e6:.2f} M (attendu ~1.8M)")
        report("Paris intra-muros %CPIS 2022",
               "ok" if 25 < cpis_pct < 40 else "warn",
               f"{cpis_pct:.1f}% (attendu ~30%)")
        report("Paris intra-muros %Ouv+Empl 2022",
               "ok" if 10 < cp_pct < 25 else "warn",
               f"{cp_pct:.1f}% (attendu ~17%)")

    if 2007 in wide and 2022 in wide:
        r07 = wide[2007]["ratio_gentrif"].median()
        r22 = wide[2022]["ratio_gentrif"].median()
        evol = (r22 / r07 - 1) * 100
        report("Ratio médian GP 2007→2022",
               "ok" if evol > 20 else "warn",
               f"{r07:.2f}→{r22:.2f} (+{evol:.0f}%, hausse attendue avec la gentrification)")


# =============================================================================
# Check 5 — Équilibre du crosswalk
# =============================================================================

def check_crosswalk_balance() -> None:
    """Σ weight groupby iris_src ≈ 1 (un IRIS est pleinement redistribué)."""
    print("\n[5] Équilibre de la table de passage IRIS")
    cw_path = DATA_RAW / "iris_crosswalk.csv"
    if not cw_path.exists():
        report("crosswalk Chabriel", "warn",
               "data/raw/iris_crosswalk.csv absent — skip")
        return
    for src, tgt in [(2017, 2022)]:
        cw = load_iris_crosswalk(source_year=src, target_year=tgt,
                                  dep_codes=DEPS_GRAND_PARIS)
        if cw is None or len(cw) == 0:
            report(f"crosswalk {src}→{tgt}", "warn", "pas de mapping")
            continue
        balance = cw.groupby("iris_src")["weight"].sum()
        pct_balanced = ((balance > 0.95) & (balance < 1.05)).mean() * 100
        status = "ok" if pct_balanced > 95 else "warn"
        report(f"crosswalk {src}→{tgt} Σpoids≈1", status,
               f"{pct_balanced:.1f}% IRIS avec balance ∈ [0.95, 1.05] "
               f"(n={balance.shape[0]:,} IRIS source)")


# =============================================================================
# Check 6 — Distribution de la trajectoire
# =============================================================================

def check_trajectory_distribution(wide: dict[int, pd.DataFrame]) -> None:
    print("\n[6] Distribution des 4 classes de trajectoire 2007→2022")
    from gentrif.indicators import classify_trajectory

    if 2007 not in wide or 2022 not in wide:
        return
    m = wide[2007][["IRIS", "ratio_gentrif"]].rename(
        columns={"ratio_gentrif": "r0"}
    ).merge(
        wide[2022][["IRIS", "ratio_gentrif"]].rename(
            columns={"ratio_gentrif": "r1"}),
        on="IRIS", how="inner",
    ).dropna()
    traj = classify_trajectory(m["r0"], m["r1"])
    vc = traj.value_counts(normalize=True) * 100
    total = len(traj.dropna())
    report("Distribution 4 classes", "ok",
           f"total={total} IRIS")
    for cls in ["Gentrification", "Relégation",
                "Consolidation bourgeoise", "Déclassement"]:
        pct = vc.get(cls, 0)
        status = "ok" if 5 < pct < 60 else "warn"
        report(f"  {cls}", status, f"{pct:.1f}%")


# =============================================================================
# Check 7 — Corrélation CSP × revenus (triangulation)
# =============================================================================

def check_csp_income_correlation(wide: dict[int, pd.DataFrame],
                                  filo: dict[int, pd.DataFrame]) -> None:
    """À l'échelle IRIS, ratio_gentrif (CSP) et rel_med_uc (revenu) devraient
    être fortement positivement corrélés — c'est la triangulation qui
    valide l'ensemble."""
    print("\n[7] Corrélation CSP × revenus (triangulation)")
    if 2022 not in wide or 2021 not in filo:
        report("CSP 2022 vs Revenu 2021", "warn", "données incomplètes")
        return
    csp = wide[2022][["IRIS", "ratio_gentrif"]]
    inc = filo[2021][["IRIS", "rel_med_uc"]]
    m = csp.merge(inc, on="IRIS", how="inner").dropna()
    r_pearson = m["ratio_gentrif"].corr(m["rel_med_uc"])
    r_spearman = m["ratio_gentrif"].corr(m["rel_med_uc"], method="spearman")
    status = "ok" if r_spearman > 0.5 else ("warn" if r_spearman > 0.3 else "fail")
    report("Corrélation IRIS CSP 2022 × revenu 2021", status,
           f"Pearson={r_pearson:.2f}, Spearman={r_spearman:.2f} (n={len(m):,})")


# =============================================================================
# Check 8 — Cohérence tendance longue
# =============================================================================

def check_long_series_trend() -> None:
    print("\n[8] Tendance longue 1968-2022 — progression monotone attendue")
    wide = _load_long_series_wide()
    if wide.empty:
        report("tendance longue", "warn", "données absentes")
        return
    years = sorted(wide["year"].unique())
    medians = {int(y): wide[wide["year"] == y]["ratio_gentrif"].median()
               for y in years}
    # Le ratio médian sur le Grand Paris doit croître monotonément 1968→2022
    # (hors petites fluctuations)
    series = [medians[y] for y in years]
    increases = sum(1 for a, b in zip(series[:-1], series[1:]) if b > a)
    total = len(series) - 1
    report("croissance monotone ratio médian", "ok" if increases >= total - 1 else "warn",
           f"{increases}/{total} pas de hausse ({', '.join(f'{y}:{medians[y]:.2f}' for y in years)})")


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    print("=" * 70)
    print("  [validate_data] Sanity checks sur les données produites")
    print("=" * 70)

    wide = _load_iris_wide()
    filo = _load_filosofi_wide()

    if not wide:
        print("  [!] data/interim/iris_wide_*.parquet absent — "
              "lance build_processed d'abord")
        return 2

    check_population_coherence(wide)
    check_value_ranges(wide, filo)
    check_temporal_consistency(wide)
    check_aggregate_benchmarks(wide)
    check_crosswalk_balance()
    check_trajectory_distribution(wide)
    check_csp_income_correlation(wide, filo)
    check_long_series_trend()

    # Récap et sortie CSV
    df = pd.DataFrame(CHECKS)
    n_fail = (df["status"] == "fail").sum()
    n_warn = (df["status"] == "warn").sum()
    n_ok = (df["status"] == "ok").sum()
    print("\n" + "=" * 70)
    print(f"  Récap : {n_ok} ok | {n_warn} warn | {n_fail} fail")
    print("=" * 70)

    out = OUT_TABLES / "validation_report.csv"
    df.to_csv(out, index=False, sep=";")
    print(f"  [csv] {out.name}")

    return 1 if n_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
