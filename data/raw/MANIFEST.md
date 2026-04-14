# Manifeste des sources primaires

Ce document recense toutes les sources brutes utilisées par le pipeline,
leur provenance exacte, et un empreinte SHA-256 vérifiable. Il fait office
de **provenance** et garantit la reproductibilité.

À chaque nouveau fetch, la commande `scripts/fetch_all.py` affiche le
checksum SHA-256 ; le reporter ci-dessous avec la date d'accès.

## Bases IRIS INSEE (Population & CSP)

| Millésime | Nom attendu            | URL de la page catalogue                                   | Date d'accès | SHA-256 (12 car.) |
|-----------|------------------------|------------------------------------------------------------|--------------|-------------------|
| 2007      | `base-ic-pop-2007*`    | <https://www.insee.fr/fr/statistiques/2028650>             | TODO         | TODO              |
| 2012      | `base-ic-pop-2012*`    | <https://www.insee.fr/fr/statistiques/2028582>             | TODO         | TODO              |
| 2017      | `base-ic-pop-2017*`    | <https://www.insee.fr/fr/statistiques/4799309>             | TODO         | TODO              |
| 2022      | `base-ic-pop-2022*`    | <https://www.insee.fr/fr/statistiques/8647014>             | TODO         | TODO              |

**Variables clés :**
- 2007-2021 (PCS 2003) : `C{YY}_POP15P_CS{3..8}`, `P{YY}_POP_FR`, `P{YY}_POP_ETR`
- 2022+ (PCS 2020) : `C22_POP15P_STAT_GSEC{XX}_{YY}`, retraités reclassés.

## Contours géographiques

| Ressource         | Fichier cache                                  | Source                                                                                                    | Date d'accès | SHA-256 |
|-------------------|------------------------------------------------|-----------------------------------------------------------------------------------------------------------|--------------|---------|
| Contours IRIS 75  | `iris_contours_75.geojson`                     | Opendatasoft `georef-france-iris`                                                                         | TODO         | TODO    |
| Contours IRIS GP  | `iris_contours_75_92_93_94.geojson`            | idem                                                                                                      | TODO         | TODO    |
| 80 quartiers      | `quartiers_paris.geojson`                      | Paris OpenData (`quartier_paris`)                                                                         | TODO         | TODO    |

## Bases FiLoSoFi IRIS (revenus fiscaux disponibles par UC)

Dispositif FiLoSoFi (*Fichier localisé social et fiscal*), source revenus
par IRIS. Voir METHODOLOGY.md §4.3 pour la discussion des ruptures de série
(RFL→FiLoSoFi 2012, évolutions méthodologiques 2018-2019).

| Millésime | Nom attendu                        | URL de la page catalogue                          | Date d'accès | SHA-256 (12 car.) |
|-----------|------------------------------------|---------------------------------------------------|--------------|-------------------|
| 2012      | `BASE_TD_FILO_DISP_IRIS_2012*`     | <https://www.insee.fr/fr/statistiques/2388225>    | TODO         | TODO              |
| 2017      | `BASE_TD_FILO_DISP_IRIS_2017*`     | <https://www.insee.fr/fr/statistiques/4797646>    | TODO         | TODO              |
| 2021      | `BASE_TD_FILO_DISP_IRIS_2021*`     | <https://www.insee.fr/fr/statistiques/7752770>    | TODO         | TODO              |

**Variables clés :**
- `DISP_MED{YY}` — médiane du niveau de vie (€/UC)
- `DISP_D1{YY}`, `DISP_D9{YY}` — 1er et 9e décile (écart interdécile)
- `DISP_TP60{YY}` — taux de pauvreté (seuil 60 % médiane nationale)
- `DISP_GI{YY}` — indice de Gini

Les URLs/IDs INSEE des pages de téléchargement sont à renseigner
manuellement dans `src/gentrif/config.py::INSEE_PAGES_FILOSOFI` — le
fetcher est cache-first et tentera un DL opportuniste si l'ID est présent.

## Sources historiques

| Ressource                    | Fichier cache                         | Source                                                       | Notes                            |
|------------------------------|---------------------------------------|--------------------------------------------------------------|----------------------------------|
| APUR Paris 1954-1999         | `apur_paris_1954_1999.pdf`            | <https://www.apur.org/sites/default/files/documents/paris.pdf> | PDF 1954-1999, saisie semi-manuelle du Tableau 3 par quartier |
| Séries harmonisées INSEE     | `base-cc-serie-historique.xlsx` ou équivalent | <https://www.insee.fr/fr/statistiques/1893185>      | Séries 1968-2022, communes, actifs par CSP. Univers : actifs ayant un emploi. Cf. METHODOLOGY §4.5. |

## Contours communes (séries longues)

| Ressource                 | Fichier cache                              | Source                                                    |
|---------------------------|--------------------------------------------|-----------------------------------------------------------|
| Contours communes GP      | `communes_contours_75_92_93_94.geojson`    | Opendatasoft `georef-france-commune`                      |

## Données à saisir manuellement

**`quartiers_csp_data.csv`** (80 lignes × 20 colonnes)

Le fichier `quartiers_csp_template.csv` est généré automatiquement par
`loaders.quartier_template()`. Renseigner les valeurs depuis le **Tableau
3** du PDF APUR pour chaque triplet (quartier, année, CSP), puis le
renommer en `quartiers_csp_data.csv`.

Colonnes (séparateur `;`) :
```
num_quartier, nom, arrondissement,
cpis_1982, prof_inter_1982, employes_1982, ouvriers_1982, pop_totale_1982,
cpis_1990, prof_inter_1990, employes_1990, ouvriers_1990, pop_totale_1990,
cpis_1999, prof_inter_1999, employes_1999, ouvriers_1999, pop_totale_1999
```

## Tables de passage

- **Table de passage IRIS 2010-2020** (Zenodo) — intégrée (cf. METHODOLOGY
  §4.4). Fichier attendu : `data/raw/iris_crosswalk.csv`.
  - Schéma tolérant : colonnes `iris_src`, `iris_dst`, `weight` (ou
    synonymes `iris_old`/`iris_new`, `IRIS_SRC`/`IRIS_DST`, etc.) ;
    séparateur `,`, `;` ou `\t` ; encodage UTF-8.
  - URL de téléchargement direct à renseigner dans
    `src/gentrif/config.py::IRIS_CROSSWALK_URL` — sinon déposer
    manuellement dans `data/raw/`.
  - Si absent : le pipeline tourne sans harmonisation (pass-through).

## Politique

- **Immuabilité** : les fichiers dans `data/raw/` ne sont jamais modifiés
  a posteriori. Toute correction passe par `data/interim/`.
- **Gitignore** : le contenu de `data/raw/` est gitignoré (volumes, droits
  de rediffusion). Seul ce `MANIFEST.md` est versionné.
