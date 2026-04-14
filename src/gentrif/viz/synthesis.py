"""
Cartes de synthèse (typologie Clerval) et cartes historiques.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from ..config import SYNTHESIS_CATEGORIES


def plot_synthesis(gdf: gpd.GeoDataFrame, label: str, path: Path) -> None:
    """
    Carte de synthèse type Figure 6 de Clerval (2010) : classification des
    unités spatiales en 6 stades de gentrification, par quantiles du
    ratio_gentrif.
    """
    fig, ax = plt.subplots(1, 1, figsize=(16, 18))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("#f0f0f0")

    d = gdf[gdf["ratio_gentrif"].notna()].copy()
    if len(d) == 0:
        plt.close()
        return

    r = d["ratio_gentrif"]
    for lbl, q_lo, q_hi, color in SYNTHESIS_CATEGORIES:
        lo = r.quantile(q_lo)
        hi = r.quantile(q_hi) if q_hi < 1 else np.inf
        sub = d[(r >= lo) & (r < hi)]
        if len(sub):
            sub.plot(ax=ax, color=color, edgecolor="white", linewidth=0.1)

    if "COM" in d.columns:
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

    ax.legend(handles=[Patch(facecolor=c, edgecolor="gray", label=l)
                       for l, _, _, c in SYNTHESIS_CATEGORIES],
              loc="lower left", fontsize=8, title="Classification")
    ax.set_axis_off()
    y = d["year"].iloc[0] if "year" in d else "?"
    ax.set_title(f"Synthèse gentrification — {label} ({y})",
                 fontsize=13, fontweight="bold", pad=15)
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
