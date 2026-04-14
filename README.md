# Gentrification à Paris et en petite couronne (1982–2022)

Actualisation et extension des travaux d'Anne Clerval publiés dans
*Cybergeo* n°505 (Clerval, 2010) avec les données INSEE les plus récentes.

## Questions de recherche

1. **Continuité Clerval** — Les dynamiques de gentrification décrites à Paris
   entre 1982 et 1999 se sont-elles poursuivies, ralenties, ou inversées
   entre 1999 et 2022 ?
2. **Extension métropolitaine** — La gentrification franchit-elle le
   périphérique, et si oui selon quelles géographies ? Quels effets de
   relocalisation des classes populaires observe-t-on vers la petite
   couronne (92, 93, 94) ?
3. **Rupture de série** — La nomenclature PCS 2020 (reclassement des
   retraités par ancienne CSP, intégrée aux variables `GSECxx_yy` du
   recensement 2022) modifie-t-elle la lecture des dynamiques par
   rapport à la PCS 2003 ?

## Métriques centrales

- **Ratio CPIS / (ouvriers + employés)** — indicateur de *substitution
  sociale* retenu par Clerval (2010, p. 7). Calculable à toutes les
  échelles et tous les millésimes, absorbe partiellement les ruptures de
  population de référence.
- **Revenu médian relatif (`rel_med_uc`)** — médiane du niveau de vie
  de l'IRIS rapportée à la médiane du périmètre, centrée sur 1.0. Source
  FiLoSoFi, 2012-2021. Permet une triangulation CSP × revenu.

Toutes deux sont injectées dans la **typologie trajectoire 2×2**
(niveau initial × évolution) qui distingue gentrification, relégation,
consolidation bourgeoise et déclassement — à la différence de la
classification en niveau, qui confondrait "beaux quartiers constitués"
(Neuilly, 16e) et "quartiers en cours de gentrification". Cf.
`METHODOLOGY.md` §2bis.

## Périmètres et échelles

| Période          | Échelle                              | Unités  | Source                                       |
|------------------|--------------------------------------|---------|----------------------------------------------|
| 1968-2021        | Communes Grand Paris                 | ~130    | INSEE séries harmonisées (§4.5)              |
| 1982, 1990, 1999 | 80 quartiers administratifs de Paris | 80      | APUR (PDF, saisie manuelle)                  |
| 2007-2022        | IRIS Grand Paris (CSP)               | ~3 900  | INSEE bases infracommunales population       |
| 2012-2021        | IRIS Grand Paris (revenus)           | ~3 900  | INSEE FiLoSoFi                               |

Une **table de passage IRIS** (Zenodo, cf. `METHODOLOGY.md` §4.4) peut
être déposée dans `data/raw/iris_crosswalk.csv` pour harmoniser les
zonages 2007↔2022. Sans elle, le pipeline tourne en pass-through.

## Pipeline en 3 étapes

```bash
pip install -e .[dev]

python scripts/fetch_all.py          # [1/3] sources -> data/raw/
python scripts/build_processed.py    # [2/3] raw -> data/processed/ (parquet long)
python scripts/generate_maps.py      # [3/3] processed -> output/figures + tables
```

Les notebooks Quarto dans `notebooks/` donnent les analyses narrées
(exploration, évolution, Grand Paris, synthèse).

## Arborescence

```
gentrification-paris/
├── README.md              Ce fichier
├── METHODOLOGY.md         Choix méthodologiques, limites, ruptures
├── REFERENCES.bib         Bibliographie (BibTeX)
├── CLAUDE.md              Contexte pour Claude Code
├── pyproject.toml         Packaging + dépendances
├── data/
│   ├── raw/               Sources primaires + MANIFEST.md (provenance)
│   ├── interim/           Données nettoyées par millésime (parquet wide)
│   └── processed/         Analysis-ready (parquet long tidy)
├── src/gentrif/           Package Python importable
│   ├── config.py          Chemins, périmètres, constantes
│   ├── schemas.py         csp_vars, csp_long_vars, filosofi_vars
│   ├── fetch.py           Téléchargement (idempotent, checksums)
│   ├── io.py              Lecture bas-niveau CSV/XLS
│   ├── indicators.py      compute_indicators, compute_income_indicators,
│   │                      classify_level, classify_trajectory
│   ├── harmonize.py       Pivot long, table de passage IRIS (apply_crosswalk_wide)
│   ├── loaders.py         load_iris, load_filosofi_iris, load_long_series,
│   │                      quartiers, contours (IRIS, communes, quartiers)
│   └── viz/               plot_map, plot_multitemp, plot_level_typology,
│                          plot_trajectory, plot_historical_maps
├── scripts/               CLI : fetch, build, maps
├── notebooks/             Analyses narrées (Quarto .qmd)
├── tests/                 Validation des indicateurs
└── output/
    ├── figures/           Cartes PNG
    ├── tables/            CSV
    └── report/            Rapport de synthèse (Quarto)
```

## Référence

Clerval, A. (2010). *Les dynamiques spatiales de la gentrification à Paris.*
Cybergeo : European Journal of Geography, **505**.
<https://doi.org/10.4000/cybergeo.23231>
