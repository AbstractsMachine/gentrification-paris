"""
Lecture bas-niveau des fichiers tabulaires INSEE (CSV/XLS) et APUR.

Les fichiers INSEE varient en séparateur, encodage, et nombre de lignes
d'en-tête selon le millésime. Ces helpers abstraient la détection.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def _looks_like_header(cols) -> bool:
    """
    Un vrai en-tête INSEE a une majorité de noms de colonnes nommés
    (non "Unnamed") et contient au moins une colonne clé attendue
    (IRIS, COM, DEP, ou un code variable CXX_ / PXX_).
    """
    named = [c for c in cols if not str(c).startswith("Unnamed")]
    if len(named) < len(cols) * 0.5:
        return False
    flat = " ".join(str(c).upper() for c in named)
    key_patterns = ["IRIS", "COM ", " COM", "DEP", "DCOMIRIS"]
    var_patterns = ["C07_", "C12_", "C17_", "C22_", "P07_", "P12_", "P17_", "P22_"]
    return (any(k in flat for k in key_patterns)
            or any(v in flat for v in var_patterns))


def read_tabular(path: Path) -> pd.DataFrame | None:
    """Lit un CSV ou un XLS/XLSX avec détection automatique du dialecte
    et de la ligne d'en-tête (bases INSEE : en-tête souvent à la ligne 5 ou 6)."""
    ext = path.suffix.lower()
    if ext == ".csv":
        for sep in [";", ",", "\t"]:
            for enc in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc,
                                     dtype=str, low_memory=False)
                    if len(df.columns) > 5 and _looks_like_header(df.columns):
                        return df
                except Exception:
                    continue
    else:
        for skip in [5, 6, 0, 4, 7]:
            try:
                df = pd.read_excel(path, skiprows=skip, dtype=str)
                if len(df.columns) > 5 and _looks_like_header(df.columns):
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
