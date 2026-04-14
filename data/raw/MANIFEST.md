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

## Sources historiques

| Ressource                    | Fichier cache                         | Source                                                       | Notes                            |
|------------------------------|---------------------------------------|--------------------------------------------------------------|----------------------------------|
| APUR Paris 1954-1999         | `apur_paris_1954_1999.pdf`            | <https://www.apur.org/sites/default/files/documents/paris.pdf> | PDF 1954-1999, saisie semi-manuelle du Tableau 3 par quartier |
| Données harmonisées INSEE    | (à télécharger manuellement)          | <https://www.insee.fr/fr/statistiques/1893185>               | Séries 1968-2022, communes, actifs par CSP |

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

## Tables de passage (à intégrer)

- **Table de passage IRIS 2010-2020** (Zenodo) : permettra l'harmonisation
  IRIS-à-IRIS inter-millésimes. À placer dans `data/raw/iris_crosswalk.csv`
  et activer via `harmonize.load_iris_crosswalk()`.

## Politique

- **Immuabilité** : les fichiers dans `data/raw/` ne sont jamais modifiés
  a posteriori. Toute correction passe par `data/interim/`.
- **Gitignore** : le contenu de `data/raw/` est gitignoré (volumes, droits
  de rediffusion). Seul ce `MANIFEST.md` est versionné.
