"""
Indicateurs de gentrification dérivés des effectifs CSP.

Métrique principale (cf. Clerval 2010, p.7) :

    ratio_gentrif = part_CPIS / part_classes_populaires
                  = pct_cpis / pct_classes_pop

Ce rapport mesure la **substitution sociale** à l'œuvre dans un territoire
donné. Il est calculable à toutes les échelles (IRIS, quartier, commune) et
à toutes les dates puisqu'il s'agit d'un rapport entre deux parts du même
univers (la population de référence — 15+ active, ménages, etc. — varie,
mais le rapport absorbe partiellement ces changements).

Voir METHODOLOGY.md §5 pour la discussion des limites de comparabilité.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def compute_indicators(df: pd.DataFrame,
                       pop_col: str = "pop15p") -> pd.DataFrame:
    """
    Ajoute les indicateurs standardisés au DataFrame `df` (modifié en place
    et retourné). `df` doit contenir les colonnes `cpis`, `employes`,
    `ouvriers`, `prof_inter`, et une colonne de population de référence.

    Colonnes ajoutées :
        pct_cpis         — part des CPIS (%)
        pct_classes_pop  — part ouvriers + employés (%)
        pct_prof_inter   — part des professions intermédiaires (%)
        ratio_gentrif    — pct_cpis / pct_classes_pop
        pct_etr          — part de population étrangère (%) si pop_etr dispo
    """
    pop = df[pop_col].replace(0, np.nan)

    df["pct_cpis"] = (df["cpis"] / pop * 100).round(2)
    df["pct_classes_pop"] = ((df["employes"] + df["ouvriers"]) / pop * 100).round(2)
    df["pct_prof_inter"] = (df["prof_inter"] / pop * 100).round(2)

    cp = df["pct_classes_pop"].replace(0, np.nan)
    df["ratio_gentrif"] = (df["pct_cpis"] / cp).round(3)

    if "pop_etr" in df and df["pop_etr"].sum() > 0:
        ptot = (df.get("pop_fr", 0) + df["pop_etr"]).replace(0, np.nan)
        df["pct_etr"] = (df["pop_etr"] / ptot * 100).round(2)

    return df


def compute_income_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit `df` avec les indicateurs dérivés de FiLoSoFi.

    Entrée attendue : colonnes `med_uc`, et optionnellement `d1`, `d9`,
    `poverty_rate`, `gini`.

    Colonnes ajoutées :
        rel_med_uc — médiane de l'IRIS rapportée à la médiane du périmètre
                     (valeur 1.0 = même niveau que la médiane locale).
        d9_d1      — rapport interdécile si d1, d9 disponibles.
    """
    med = df["med_uc"].replace(0, np.nan)
    ref = med.median(skipna=True)
    df["rel_med_uc"] = (med / ref).round(3) if ref and not np.isnan(ref) else np.nan
    if {"d1", "d9"}.issubset(df.columns):
        d1 = df["d1"].replace(0, np.nan)
        df["d9_d1"] = (df["d9"] / d1).round(2)
    return df


def classify_level(ratio: pd.Series,
                   quantiles: list[tuple[str, float, float]] | None = None
                   ) -> pd.Series:
    """
    Classification en *niveau* : attribue à chaque observation une catégorie
    de géographie sociale basée sur le `ratio_gentrif` à une date donnée,
    par découpage en quantiles empiriques.

    **Attention** : cette classification décrit un état social, pas un
    processus. Elle mettra Neuilly et le 16e dans la catégorie la plus
    élevée à n'importe quelle date — ce qui ne signifie pas qu'ils sont en
    cours de gentrification. Pour caractériser la gentrification comme
    processus de substitution sociale, utiliser `classify_trajectory`
    (cf. METHODOLOGY.md §2bis).
    """
    from .config import LEVEL_CATEGORIES

    cats = quantiles or [(lbl, lo, hi) for lbl, lo, hi, _ in LEVEL_CATEGORIES]
    thresholds = {lbl: (ratio.quantile(lo), ratio.quantile(hi) if hi < 1 else np.inf)
                  for lbl, lo, hi in cats}
    labels = pd.Series(index=ratio.index, dtype="object")
    for lbl, (lo, hi) in thresholds.items():
        mask = (ratio >= lo) & (ratio < hi)
        labels.loc[mask] = lbl
    return labels


def classify_trajectory(ratio_t0: pd.Series,
                        ratio_t1: pd.Series,
                        level_quantile: float = 0.5,
                        delta_threshold: float = 0.0) -> pd.Series:
    """
    Classification en *trajectoire* : typologie 2×2 croisant le niveau
    initial du `ratio_gentrif` avec son évolution entre t0 et t1.

    Les quatre classes (cf. METHODOLOGY.md §2bis) :

    - **Gentrification** : niveau bas à t0 → hausse (substitution sociale).
    - **Relégation** : niveau bas à t0 → stagnation ou baisse.
    - **Consolidation bourgeoise** : niveau haut à t0 → hausse (les riches
      le restent et se renforcent).
    - **Déclassement** : niveau haut à t0 → baisse.

    Paramètres
    ----------
    ratio_t0, ratio_t1 : pd.Series
        `ratio_gentrif` aux deux dates comparées, indexés de manière alignée.
    level_quantile : float
        Seuil de coupure du niveau initial, exprimé en quantile (défaut 0.5,
        soit la médiane du périmètre à t0).
    delta_threshold : float
        Seuil de variation (en unités de ratio) au-delà duquel l'évolution
        est considérée comme "à la hausse". Défaut 0 (toute hausse compte).

    Retourne
    --------
    pd.Series de labels (string), NaN aux observations de données manquantes.
    """
    if not ratio_t0.index.equals(ratio_t1.index):
        ratio_t1 = ratio_t1.reindex(ratio_t0.index)

    level_cut = ratio_t0.quantile(level_quantile)
    high_level = ratio_t0 >= level_cut
    rising = (ratio_t1 - ratio_t0) > delta_threshold

    labels = pd.Series(index=ratio_t0.index, dtype="object")
    labels[~high_level &  rising] = "Gentrification"
    labels[~high_level & ~rising] = "Relégation"
    labels[ high_level &  rising] = "Consolidation bourgeoise"
    labels[ high_level & ~rising] = "Déclassement"
    labels[ratio_t0.isna() | ratio_t1.isna()] = np.nan
    return labels
