# Gentrification Paris & Grand Paris

## Contexte
Actualisation et extension des travaux d'Anne Clerval (2010), "Les dynamiques spatiales de la gentrification à Paris", Cybergeo n°505. https://doi.org/10.4000/cybergeo.23231

## Objectif
Reproduire et étendre l'analyse de la gentrification parisienne avec les données actuelles :
- **1982-1999** : 80 quartiers administratifs de Paris (source APUR)
- **2007-2022** : IRIS (~900 Paris, ~3000 petite couronne) (source INSEE)
- **1968-2022** : données harmonisées communales pour la tendance longue
- **Périmètres** : Paris intra-muros (75) + Petite couronne (92, 93, 94)

## Métrique principale
**Ratio CPIS / (ouvriers + employés)** — indicateur de substitution sociale, calculable à toutes les échelles et toutes les dates.

### Ruptures de série à gérer
- 2007-2021 : ancienne PCS, variables `C{YY}_POP15P_CS{N}`, retraités = CS7 (non reclassés)
- 2022+ : PCS 2020, variables `C22_POP15P_STAT_GSEC{XX}_{YY}`, retraités reclassés par ancienne CSP (GSEC13_23 = CPIS actifs + retraités ex-CPIS)
- 1982-1999 : données APUR au niveau quartier, population des ménages classée par CSP de la personne de référence

## Sources de données

| Données | URL | Format |
|---------|-----|--------|
| IRIS Population 2007 | https://www.insee.fr/fr/statistiques/2028650 | XLS |
| IRIS Population 2012 | https://www.insee.fr/fr/statistiques/2028582 | XLS |
| IRIS Population 2017 | https://www.insee.fr/fr/statistiques/4799309 | CSV |
| IRIS Population 2022 | https://www.insee.fr/fr/statistiques/8647014 | CSV |
| APUR Paris 1954-1999 | https://www.apur.org/sites/default/files/documents/paris.pdf | PDF |
| Harmonisées 1968-2022 | https://www.insee.fr/fr/statistiques/1893185 | XLS |
| Contours IRIS | https://public.opendatasoft.com/explore/dataset/georef-france-iris/ | GeoJSON |
| Contours 80 quartiers | https://opendata.paris.fr/explore/dataset/quartier_paris/ | GeoJSON |

## Structure du projet
```
data/             # Données téléchargées (gitignored)
output/           # Cartes et CSV produits (gitignored)
main.py           # Script principal unique
requirements.txt
CLAUDE.md         # Ce fichier
.gitignore
```

## Commandes
```bash
pip install -r requirements.txt
python main.py                    # Lance tout
python main.py --historical       # Module historique seul
python main.py --iris             # Module IRIS seul
```

## Stack
Python 3.10+, pandas, geopandas, matplotlib, mapclassify, pdfplumber, requests
