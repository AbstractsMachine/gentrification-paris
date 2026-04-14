"""
Lecture bas-niveau des fichiers tabulaires INSEE (CSV/XLS) et APUR.

Les fichiers INSEE varient en séparateur, encodage, et nombre de lignes
d'en-tête selon le millésime. Ces helpers abstraient la détection.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_tabular(path: Path) -> pd.DataFrame | None:
    """Lit un CSV ou un XLS/XLSX avec détection automatique du dialecte."""
    ext = path.suffix.lower()
    if ext == ".csv":
        for sep in [";", ",", "\t"]:
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc,
                                     dtype=str, low_memory=False)
                    if len(df.columns) > 5:
                        return df
                except Exception:
                    continue
    else:
        for skip in [0, 5, 6]:
            try:
                df = pd.read_excel(path, skiprows=skip, dtype=str)
                if len(df.columns) > 5:
                    return df
            except Exception:
                continue
    return None


def col_find(df: pd.DataFrame, target: str) -> str | None:
    """
    Recherche insensible à la casse et aux underscores d'une colonne.
    Retourne le nom exact tel qu'il figure dans `df`, ou None.
    """
    for c in df.columns:
        if c.strip().upper() == target.upper():
            return c
    target_flat = target.upper().replace("_", "")
    for c in df.columns:
        if target_flat in c.upper().replace("_", ""):
            return c
    return None
