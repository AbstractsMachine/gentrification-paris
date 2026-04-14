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


def classify_gentrification(ratio: pd.Series,
                            quantiles: list[tuple[str, float, float]] | None = None
                            ) -> pd.Series:
    """
    Attribue à chaque observation une catégorie de synthèse basée sur le
    `ratio_gentrif`, par découpage en quantiles empiriques.

    Utile pour la carte de synthèse type Figure 6 de Clerval (2010).
    """
    from .config import SYNTHESIS_CATEGORIES

    cats = quantiles or [(lbl, lo, hi) for lbl, lo, hi, _ in SYNTHESIS_CATEGORIES]
    thresholds = {lbl: (ratio.quantile(lo), ratio.quantile(hi) if hi < 1 else np.inf)
                  for lbl, lo, hi in cats}
    labels = pd.Series(index=ratio.index, dtype="object")
    for lbl, (lo, hi) in thresholds.items():
        mask = (ratio >= lo) & (ratio < hi)
        labels.loc[mask] = lbl
    return labels
