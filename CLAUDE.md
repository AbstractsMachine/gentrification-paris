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
  config.py         Chemins, périmètres, 80 quartiers, catégories synthèse
  schemas.py        Mapping CSP par année (PCS 2003 vs PCS 2020)
  fetch.py          Téléchargement + checksums SHA-256
  io.py             Lecture bas-niveau CSV/XLS (dialectes INSEE)
  indicators.py     compute_indicators(), classify_gentrification()
  harmonize.py      to_long() -> schéma canonique, stub crosswalk IRIS
  loaders.py        load_iris(), load_historical_quartiers(), contours
  viz/              plot_map, plot_multitemp, plot_synthesis, hist_maps

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

## Extensions à venir (cf. `METHODOLOGY.md` §7)
1. Intégration table de passage IRIS (Zenodo)
2. Tendance longue 1968-2022 (communes, séries harmonisées INSEE)
3. Analyse LISA / Moran local
4. Croisement FILOSOFI + DVF
5. Modélisation de la relocalisation des classes populaires en petite couronne
