"""
Cartes de synthèse (typologie trajectoire 2×2 + niveau) et cartes historiques.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from ..config import LEVEL_CATEGORIES, TRAJECTORY_CATEGORIES
from ..indicators import classify_trajectory


def _annotate_arrondissements(ax, d: gpd.GeoDataFrame) -> None:
    if "COM" not in d.columns:
        return
    try:
        cm = d.dissolve(by="COM")
        cm.boundary.plot(ax=ax, edgecolor="black", linewidth=0.8)
        for idx, row in cm.iterrows():
            c = row.geometry.centroid
            s = str(idx)
            if s.startswith("751"):
                ax.annotate(
                    f"{s[-2:].lstrip('0')}e", xy=(c.x, c.y),
                    ha="center", va="center", fontsize=6,
                    fontweight="bold", color="white",
                    path_effects=[pe.withStroke(linewidth=2, foreground="black")],
                )
    except Exception:
        pass


def plot_level_typology(gdf: gpd.GeoDataFrame, label: str, path: Path) -> None:
    """
    Carte de **géographie sociale** : classification en niveau du
    `ratio_gentrif` à une date donnée, par quantiles empiriques.

    Cette carte décrit un *état* (qui est riche, qui est pauvre), pas un
    processus. Elle n'identifie pas les quartiers en cours de
    gentrification — pour cela, utiliser `plot_trajectory`.
    """
    fig, ax = plt.subplots(1, 1, figsize=(16, 18))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("#f0f0f0")

    d = gdf[gdf["ratio_gentrif"].notna()].copy()
    if len(d) == 0:
        plt.close()
        return

    r = d["ratio_gentrif"]
    for lbl, q_lo, q_hi, color in LEVEL_CATEGORIES:
        lo = r.quantile(q_lo)
        hi = r.quantile(q_hi) if q_hi < 1 else np.inf
        sub = d[(r >= lo) & (r < hi)]
        if len(sub):
            sub.plot(ax=ax, color=color, edgecolor="white", linewidth=0.1)

    _annotate_arrondissements(ax, d)

    ax.legend(handles=[Patch(facecolor=c, edgecolor="gray", label=l)
                       for l, _, _, c in LEVEL_CATEGORIES],
              loc="lower left", fontsize=8, title="Géographie sociale")
    ax.set_axis_off()
    y = d["year"].iloc[0] if "year" in d else "?"
    ax.set_title(f"Géographie sociale (niveau) — {label} ({y})",
                 fontsize=13, fontweight="bold", pad=15)
    fig.text(0.5, 0.01,
             "Niveau du ratio CPIS / classes populaires — décrit un état, "
             "pas un processus.",
             ha="center", fontsize=7, color="gray", style="italic")
    plt.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"    [map] {path.name}")


def plot_trajectory(gdf_t0: gpd.GeoDataFrame,
                    gdf_t1: gpd.GeoDataFrame,
                    label: str,
                    path: Path,
                    key: str = "IRIS",
                    level_quantile: float = 0.5,
                    delta_threshold: float = 0.0) -> None:
    """
    Carte de **synthèse type Figure 6 de Clerval** : typologie 2×2 croisant
    le niveau initial du `ratio_gentrif` (t0) avec son évolution (t0 → t1).

    Seule carte qui caractérise véritablement le *processus* de
    gentrification (cf. METHODOLOGY.md §2bis).
    """
    cols_needed = {key, "ratio_gentrif"}
    if not cols_needed.issubset(gdf_t0.columns) or \
       not cols_needed.issubset(gdf_t1.columns):
        return

    merged = gdf_t1[[key, "geometry", "ratio_gentrif"]].rename(
        columns={"ratio_gentrif": "ratio_t1"}
    ).merge(
        gdf_t0[[key, "ratio_gentrif"]].rename(
            columns={"ratio_gentrif": "ratio_t0"}),
        on=key, how="inner",
    )
    if "COM" in gdf_t1.columns:
        merged = merged.merge(gdf_t1[[key, "COM"]], on=key, how="left")
    merged = gpd.GeoDataFrame(merged, geometry="geometry",
                              crs=gdf_t1.crs)

    merged["trajectory"] = classify_trajectory(
        merged["ratio_t0"], merged["ratio_t1"],
        level_quantile=level_quantile,
        delta_threshold=delta_threshold,
    )

    fig, ax = plt.subplots(1, 1, figsize=(16, 18))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("#f0f0f0")

    na_mask = merged["trajectory"].isna()
    if na_mask.any():
        merged[na_mask].plot(ax=ax, color="lightgray",
                             edgecolor="white", linewidth=0.1)
    for lbl, color in TRAJECTORY_CATEGORIES:
        sub = merged[merged["trajectory"] == lbl]
        if len(sub):
            sub.plot(ax=ax, color=color, edgecolor="white", linewidth=0.1)

    _annotate_arrondissements(ax, merged)

    y0 = gdf_t0["year"].iloc[0] if "year" in gdf_t0 else "t0"
    y1 = gdf_t1["year"].iloc[0] if "year" in gdf_t1 else "t1"
    ax.legend(handles=[Patch(facecolor=c, edgecolor="gray", label=l)
                       for l, c in TRAJECTORY_CATEGORIES],
              loc="lower left", fontsize=9,
              title=f"Trajectoire {y0} → {y1}")
    ax.set_axis_off()
    ax.set_title(f"Trajectoires socio-spatiales — {label} ({y0} → {y1})",
                 fontsize=13, fontweight="bold", pad=15)
    fig.text(0.5, 0.01,
             "Niveau initial (médiane à t0) × évolution du ratio CPIS / "
             "classes populaires.",
             ha="center", fontsize=7, color="gray", style="italic")
    plt.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"    [map] {path.name}")


def plot_historical_maps(qdata: dict[int, pd.DataFrame],
                         contours: gpd.GeoDataFrame,
                         output_dir: Path) -> None:
    """
    Cartes multi-temporelles à l'échelle des 80 quartiers administratifs
    (période Clerval 1982-1999).
    """
    years = sorted(qdata.keys())
    if not years or contours is None:
        return

    for indicator, title_base, cmap, fname in [
        ("pct_classes_pop", "Ouvriers + employés", "Reds", "hist_classes_pop"),
        ("pct_cpis", "CPIS", "Blues", "hist_cpis"),
    ]:
        n = len(years)
        fig, axes = plt.subplots(1, n, figsize=(7 * n, 9))
        fig.patch.set_facecolor("white")
        if n == 1:
            axes = [axes]

        merged_all = [contours.merge(qdata[y], on="num_quartier") for y in years]
        all_v = pd.concat([m[indicator].dropna() for m in merged_all if indicator in m])
        vmin, vmax = (all_v.quantile(0.02), all_v.quantile(0.98)) if len(all_v) else (0, 100)

        for ax, y, mg in zip(axes, years, merged_all):
            ax.set_facecolor("#f5f5f5")
            if indicator in mg and not mg[indicator].isna().all():
                mg.plot(column=indicator, ax=ax, cmap=cmap,
                        vmin=vmin, vmax=vmax, edgecolor="black", linewidth=0.3,
                        missing_kwds=dict(color="lightgray"))
                arr_c = "arrondissement_x" if "arrondissement_x" in mg else "arrondissement"
                if arr_c in mg.columns:
                    try:
                        mg.dissolve(by=arr_c).boundary.plot(
                            ax=ax, edgecolor="black", linewidth=1.2)
                    except Exception:
                        pass
                ax.text(0.98, 0.02, f"Moy: {mg[indicator].mean():.1f}%",
                        transform=ax.transAxes, ha="right", va="bottom",
                        fontsize=9, bbox=dict(boxstyle="round",
                                              facecolor="white", alpha=0.8))
            ax.set_axis_off()
            ax.set_title(str(y), fontsize=13, fontweight="bold")

        sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin, vmax))
        sm.set_array([])
        fig.colorbar(sm, ax=axes, orientation="horizontal",
                     fraction=0.03, pad=0.05, shrink=0.5).set_label("%")
        fig.suptitle(f"{title_base} — 80 quartiers de Paris",
                     fontsize=14, fontweight="bold", y=0.98)
        fig.text(0.5, 0.01, "Source: INSEE RGP | APUR | D'après Clerval (2010)",
                 ha="center", fontsize=7, color="gray")
        p = output_dir / f"{fname}_{'_'.join(map(str, years))}.png"
        plt.savefig(p, dpi=180, bbox_inches="tight", facecolor="white")
        plt.close()
        print(f"    [map] {p.name}")
