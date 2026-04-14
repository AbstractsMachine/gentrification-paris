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

## Métrique centrale

**Ratio CPIS / (ouvriers + employés)** — indicateur de *substitution
sociale* retenu par Clerval (2010, p. 7). Il est calculable à toutes les
échelles et tous les millésimes, et absorbe partiellement les ruptures
de population de référence (cf. `METHODOLOGY.md`).

## Périmètres et échelles

| Période     | Échelle            | Unités  | Source                          |
|-------------|--------------------|---------|---------------------------------|
| 1982, 1990, 1999 | 80 quartiers administratifs de Paris | 80      | APUR (PDF, saisie manuelle)     |
| 2007-2022   | IRIS Grand Paris   | ~3 900  | INSEE bases infracommunales     |
| 1968-2022   | Communes Grand Paris | ~130  | INSEE séries harmonisées (optionnel, extension) |

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
│   ├── schemas.py         Variables CSP par année (PCS 2003 / PCS 2020)
│   ├── fetch.py           Téléchargement (idempotent, checksums)
│   ├── io.py              Lecture bas-niveau CSV/XLS
│   ├── indicators.py      Calcul des parts et du ratio_gentrif
│   ├── harmonize.py       Pivot long, table de passage IRIS
│   ├── loaders.py         Chargeurs haut-niveau par source
│   └── viz/               Cartes choroplèthes, synthèse, historique
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
