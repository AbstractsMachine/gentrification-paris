#!/usr/bin/env python3
"""
Étape 1 du pipeline — Téléchargement de toutes les sources primaires
dans data/raw/.

Idempotent : les fichiers déjà présents ne sont pas retéléchargés.
Voir data/raw/MANIFEST.md pour la liste exhaustive et la provenance.
"""
from __future__ import annotations

from gentrif.config import (
    DEPS_GRAND_PARIS,
    DEPS_PARIS,
    FILOSOFI_YEARS,
    IRIS_YEARS,
)
from gentrif.fetch import (
    fetch_apur_pdf,
    fetch_commune_contours,
    fetch_filosofi_year,
    fetch_iris_contours,
    fetch_iris_crosswalk,
    fetch_iris_year,
    fetch_long_series,
    fetch_quartier_contours,
)


def main() -> None:
    print("=" * 60)
    print("  [1/3] FETCH — sources primaires vers data/raw/")
    print("=" * 60)

    print("\n-- Bases IRIS INSEE")
    for y in IRIS_YEARS:
        fetch_iris_year(y)

    print("\n-- Bases FiLoSoFi IRIS (revenus)")
    for y in FILOSOFI_YEARS:
        fetch_filosofi_year(y)

    print("\n-- Contours IRIS")
    fetch_iris_contours(DEPS_PARIS)
    fetch_iris_contours(DEPS_GRAND_PARIS)

    print("\n-- Contours communes (pour séries longues)")
    fetch_commune_contours(DEPS_GRAND_PARIS)

    print("\n-- Contours 80 quartiers (APUR / Paris opendata)")
    fetch_quartier_contours()

    print("\n-- Séries harmonisées longues INSEE (1968-2022, communes)")
    fetch_long_series()

    print("\n-- Table de passage IRIS (Zenodo)")
    fetch_iris_crosswalk()

    print("\n-- PDF APUR 1954-1999")
    fetch_apur_pdf()

    print("\n[done] cf. data/raw/MANIFEST.md pour la provenance.")


if __name__ == "__main__":
    main()
