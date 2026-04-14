"""
Cartes choroplèthes et multi-temporelles.

Conventions graphiques (cf. Clerval 2010) :
- Bleu séquentiel (Blues) pour la part des CPIS / indicateurs de gentrification
- Rouge séquentiel (Reds) pour la part des classes populaires
- Divergent RdBu_r pour les cartes d'évolution (Δ)
- Discrétisation en quantiles (k=7) pour favoriser la lecture des fronts
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_map(gdf: gpd.GeoDataFrame, col: str, title: str,
             path: Path, cmap: str = "Blues",
             diverging: bool = False) -> None:
    """Carte choroplèthe unique."""
    fig, ax = plt.subplots(1, 1, figsize=(14, 16))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("#f5f5f5")

    d = gdf[gdf[col].notna() & np.isfinite(gdf[col])]
    if len(d) == 0:
        plt.close()
        return

    if diverging:
        mx = max(abs(d[col].quantile(0.02)), abs(d[col].quantile(0.98)))
        d.plot(column=col, ax=ax, cmap="RdBu_r", vmin=-mx, vmax=mx,
               legend=True, edgecolor="white", linewidth=0.1,
               legend_kwds=dict(label="Δ pts %", shrink=0.5,
                                orientation="horizontal", pad=0.02))
    else:
        try:
            import mapclassify  # noqa: F401
            d.plot(column=col, ax=ax, cmap=cmap, scheme="quantiles", k=7,
                   legend=True, edgecolor="white", linewidth=0.1,
                   legend_kwds=dict(loc="lower left", fontsize=7, title="%"))
        except Exception:
            d.plot(column=col, ax=ax, cmap=cmap,
                   vmin=d[col].quantile(0.02), vmax=d[col].quantile(0.98),
                   legend=True, edgecolor="white", linewidth=0.1,
                   legend_kwds=dict(label="%", shrink=0.5,
                                    orientation="horizontal", pad=0.02))

    if "COM" in d.columns:
        try:
            d.dissolve(by="COM").boundary.plot(
                ax=ax, edgecolor="black", linewidth=0.6, alpha=0.7)
        except Exception:
            pass

    ax.set_axis_off()
    ax.set_title(title, fontsize=13, fontweight="bold", pad=15)
    scheme_note = ("Échelle divergente centrée sur 0, bornée au q2/q98 "
                   "du périmètre affiché."
                   if diverging else
                   "Discrétisation en quantiles calculés sur le périmètre "
                   "affiché (pas comparable entre cartes de périmètres "
                   "différents).")
    fig.text(0.5, 0.035, scheme_note, ha="center", fontsize=7,
             color="#444", style="italic")
    fig.text(0.5, 0.015, "Source: INSEE | D'après Clerval (2010)",
             ha="center", fontsize=7, color="gray")
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"    [map] {path.name}")


def plot_multitemp(gdfs: dict[int, gpd.GeoDataFrame], col: str,
                   title: str, path: Path, cmap: str = "Blues") -> None:
    """Série de cartes côte à côte sur plusieurs millésimes, échelle commune."""
    years = sorted(gdfs.keys())
    n = len(years)
    if n == 0:
        return

    fig, axes = plt.subplots(1, n, figsize=(7 * n, 9))
    fig.patch.set_facecolor("white")
    if n == 1:
        axes = [axes]

    vals = pd.concat([g[col].dropna() for g in gdfs.values()])
    vmin, vmax = vals.quantile(0.02), vals.quantile(0.98)

    for ax, y in zip(axes, years):
        ax.set_facecolor("#f5f5f5")
        d = gdfs[y][gdfs[y][col].notna()]
        d.plot(column=col, ax=ax, cmap=cmap, vmin=vmin, vmax=vmax,
               edgecolor="white", linewidth=0.08)
        if "COM" in d.columns:
            try:
                d.dissolve(by="COM").boundary.plot(
                    ax=ax, edgecolor="black", linewidth=0.4, alpha=0.6)
            except Exception:
                pass
        ax.set_axis_off()
        ax.set_title(str(y), fontsize=12, fontweight="bold")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin, vmax))
    sm.set_array([])
    fig.colorbar(sm, ax=axes, orientation="horizontal",
                 fraction=0.03, pad=0.05, shrink=0.6).set_label("%")
    fig.suptitle(title, fontsize=14, fontweight="bold", y=0.98)
    fig.text(0.5, 0.005,
             "Échelle linéaire commune aux années (q2/q98 sur l'ensemble "
             "des millésimes du périmètre affiché) — comparable dans le "
             "temps, pas comparable entre périmètres.",
             ha="center", fontsize=7, color="#444", style="italic")
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"    [map] {path.name}")
