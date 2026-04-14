# Gentrification Paris & Grand Paris — Contexte Claude Code

Projet d'actualisation et d'extension des travaux de Clerval (2010),
*Les dynamiques spatiales de la gentrification à Paris*, Cybergeo n°505
(<https://doi.org/10.4000/cybergeo.23231>).

## Objectif scientifique
Prolonger l'analyse quantitative de Clerval dans l'espace (Paris + petite
couronne) et dans le temps (1982 → 2022), en documentant explicitement
les choix méthodologiques (voir `METHODOLOGY.md`).

## Architecture

```
src/gentrif/        Package importable
  config.py         Chemins, périmètres, 80 quartiers, catégories niveau + trajectoire
  schemas.py        Mapping CSP par année (PCS 2003 vs PCS 2020)
  fetch.py          Téléchargement + checksums SHA-256
  io.py             Lecture bas-niveau CSV/XLS (dialectes INSEE)
  indicators.py     compute_indicators(), compute_income_indicators(), classify_level/trajectory()
  harmonize.py      to_long(), load_iris_crosswalk, apply_crosswalk_wide, harmonize_iris
  loaders.py        load_iris, load_filosofi_iris, load_long_series, contours
  viz/              plot_map, plot_multitemp, plot_level_typology, plot_trajectory, hist_maps

scripts/            CLI en 3 étapes : fetch_all → build_processed → generate_maps
notebooks/          Analyses narrées (Quarto .qmd)
tests/              pytest — validation des indicateurs
data/raw|interim|processed/
output/figures|tables|report/
```

## Principes directeurs
- **Raw jamais modifié** : toute transformation passe par `interim/`
  puis `processed/`.
- **Format long canonique** : `(year, geo_level, geo_code, geo_name,
  indicator, value)` en parquet dans `data/processed/`.
- **Métrique centrale** : `ratio_gentrif = pct_cpis / pct_classes_pop`
  (substitution sociale).
- **État ≠ processus** (`METHODOLOGY.md` §2bis) : la carte de référence
  pour caractériser la gentrification est la **typologie trajectoire 2×2**
  (niveau initial × évolution), pas la classification en niveau à une
  date donnée — qui mettrait Neuilly et le 16e en tête.
- **Ruptures de série documentées** (`METHODOLOGY.md` §4) : PCS 2020 en
  2022 agrège retraités reclassés → surévaluation locale de la croissance
  CPIS à interpréter.
- **Provenance** : chaque source est listée dans `data/raw/MANIFEST.md`
  avec URL, date d'accès, SHA-256.

## Commandes
```bash
pip install -e .[dev]
python scripts/fetch_all.py
python scripts/build_processed.py
python scripts/generate_maps.py
pytest
```

## Déjà en place
- Typologie trajectoire 2×2 (état vs processus) — carte de référence
  (`classify_trajectory`, `plot_trajectory`) — cf. METHODOLOGY §2bis
- FiLoSoFi intégré — triangulation CSP × revenu via `rel_med_uc`
  (`filosofi_vars`, `load_filosofi_iris`, `compute_income_indicators`) —
  cf. METHODOLOGY §4.3
- Table de passage IRIS — harmonisation additive si crosswalk présent
  (`harmonize.apply_crosswalk_wide`) — cf. METHODOLOGY §4.4
- Tendance longue 1968-2022 (communes) — `csp_long_vars`,
  `load_long_series`, `run_long_series_maps` — cf. METHODOLOGY §4.5
- 3 périmètres : Paris intra-muros, petite couronne, Grand Paris
- Séparation stricte raw / interim / processed, parquet long canonique

## Extensions à venir (cf. `METHODOLOGY.md` §7)
1. Analyse LISA / Moran local
2. DVF (prix immobiliers, agrégation à l'IRIS)
3. Diplômes comme 3e axe de triangulation
4. Modélisation de la relocalisation des classes populaires en petite couronne
