# Méthodologie

Ce document précise les choix opérés dans le pipeline, leurs justifications,
et les limites assumées. Il est destiné à rendre les résultats défendables
en contexte académique.

## 1. Positionnement par rapport à Clerval (2010)

Clerval (2010) mobilise conjointement :

- une analyse **quantitative** à l'échelle IRIS sur trois millésimes (1982,
  1990, 1999), à partir de données détaillées fournies par l'APUR
  (pour 1982) et l'INSEE (pour 1990 et 1999) ;
- une analyse **diachronique** à l'échelle communale/arrondissement ;
- une démarche **qualitative** de terrain, située dans le nord-est parisien.

Le présent travail **reprend et prolonge le volet quantitatif**. Il
n'entend pas se substituer au volet qualitatif, lequel reste indispensable
pour caractériser les dynamiques sociales concrètes (trajectoires
résidentielles, rapports aux espaces publics, mobilisations locales).
Les résultats produits ici sont donc à lire comme une **cartographie
structurelle** de la substitution sociale, qui identifie des territoires
dont il conviendrait ensuite d'enquêter la vie sociale.

## 2. Métrique principale : ratio de substitution sociale

Pour chaque unité spatiale `i` et millésime `t` :

```
ratio_gentrif(i, t) = part_CPIS(i, t) / part_classes_populaires(i, t)
                    = pct_cpis / pct_(ouvriers + employés)
```

Ce rapport est privilégié sur la part brute des CPIS pour trois raisons :

1. **Robustesse aux ruptures de population de référence.** Il s'agit d'un
   rapport entre deux parts du même univers : qu'on normalise par la
   population totale, active, ou 15 ans et plus, le rapport est inchangé.
   Cela permet une comparaison approximative entre des sources aux
   univers légèrement différents (ménages / individus / actifs).
2. **Captation de la dynamique de substitution.** Clerval (2010, §11) :
   « La gentrification désigne le remplacement progressif des catégories
   populaires par des catégories sociales supérieures dans un quartier
   donné. » Le rapport `CPIS/populaires` rend compte directement de ce
   remplacement : un passage de 0,5 à 3,0 est le marqueur type d'une
   gentrification avancée.
3. **Continuité temporelle.** Le rapport est calculable à toutes les
   échelles (IRIS, quartier, commune) et tous les millésimes disponibles,
   y compris lorsque les catégories sous-jacentes ne sont pas
   rigoureusement identiques.

### Indicateurs complémentaires

- `pct_cpis`, `pct_classes_pop`, `pct_prof_inter` (parts brutes) pour
  l'interprétation locale.
- `pct_etr` (population étrangère), utilisé par Clerval comme révélateur
  de *pôles de résistance* à la gentrification.
- Carte de synthèse : classification en 6 stades par quantiles du
  `ratio_gentrif` (cf. `config.py::SYNTHESIS_CATEGORIES`).

## 3. Organisation des données (raw / interim / processed)

Convention Cookiecutter Data Science :

- **`data/raw/`** — sources primaires, jamais modifiées. Provenance
  documentée dans `MANIFEST.md` (URL, date d'accès, checksum).
- **`data/interim/`** — données nettoyées et indicateurs calculés,
  format wide, un fichier par millésime (`iris_wide_{year}.parquet`).
- **`data/processed/`** — tables analysis-ready en **format long tidy**
  selon le schéma canonique :

      (year, geo_level, geo_code, geo_name, indicator, value)

Ce format permet un accès uniforme en aval (filtres, joints, facets) et
correspond aux standards de la géographie quantitative contemporaine
(Lovelace et al., *Geocomputation with R*, 2019).

## 4. Ruptures de série à gérer

### 4.1. Nomenclature PCS 2020 (recensement 2022)

Les variables IRIS changent de forme à partir de 2022 :

| Période     | Nomenclature | CPIS                  | Classes populaires               |
|-------------|--------------|-----------------------|----------------------------------|
| 2007-2021   | PCS 2003     | `C{YY}_POP15P_CS3`    | `CS5 + CS6`                      |
| 2022+       | PCS 2020     | `C22_POP15P_STAT_GSEC13_23` (CPIS actifs + retraités ex-CPIS) | `GSEC15_25 + GSEC16_26` |

Les variables `GSEC{X}_{Y}` de la PCS 2020 agrègent **actifs et retraités
reclassés selon leur ancienne CSP**, ce qui rapproche — incidemment — la
pratique INSEE de la méthodologie Clerval (2010), qui reclassait
manuellement les retraités via la CSP de la personne de référence du
ménage.

Conséquence pratique : la comparaison 2017-2022 surestime légèrement la
croissance des CPIS dans les territoires à forte part de retraités
ex-cadres (7e, 16e, 17e arrondissements), parce qu'en 2022 ils sont
intégrés au décompte des CPIS alors qu'en 2017 ils étaient hors champ
(CS7 monolithique). Cette surévaluation est **structurelle et non
comportementale** : elle ne traduit pas une gentrification supplémentaire
mais un changement de mesure. Elle sera signalée explicitement dans les
lectures des cartes 2017 vs 2022.

### 4.2. Population de référence

- 1982-1999 (APUR) : population des ménages, classée par CSP de la
  personne de référence. Enfants inclus.
- 2007-2021 (IRIS) : population de 15 ans ou plus, chaque individu
  classé par sa CSP propre.
- 2022 (IRIS) : idem 15 ans ou plus, avec retraités reclassés.

**Comparaison stricte 1982-2022 impossible** à l'identique. Le recours
au **ratio** (plutôt qu'à des parts brutes) atténue l'effet, mais
n'annule pas totalement l'écart. Pour les analyses de tendance longue,
les données harmonisées INSEE (1968-2022, communes) sont restreintes
aux actifs 25-54 ans — un champ mobilisable pour une version *long-run*
non encore implémentée (cf. §7).

### 4.3. Zonages IRIS évolutifs

Les codes IRIS sont révisés périodiquement (refontes 2008, 2015…). Une
comparaison IRIS-à-IRIS 2007 vs 2022 est donc approximative pour les
territoires redécoupés. Le dépôt Zenodo *"Harmonized INSEE
socio-demographic IRIS-level data and IRIS conversion file (2010-2020)"*
fournit une table de passage — son intégration est prévue
(`harmonize.py::load_iris_crosswalk`).

En l'état, l'essentiel des comparaisons se lit à l'**échelle du quartier
perceptible** (grappe d'IRIS contigus), ce qui relativise l'impact des
refontes ponctuelles.

## 5. Limites assumées

- **Secret statistique**. Les IRIS de très faible population voient
  certaines variables masquées ou arrondies par l'INSEE. Ces cas
  n'ont pas été exclus mais produiront des valeurs aberrantes en
  queue de distribution.
- **Résolution temporelle IRIS pour la période Clerval**. Les fichiers
  IRIS 1982, 1990, 1999 ne sont pas en open data. Nous utilisons les
  80 quartiers administratifs de Paris (APUR) comme *proxy de résolution*
  sur cette période ; le grain intermédiaire (plus fin que l'arrondissement,
  moins fin que l'IRIS) reste utile pour lire les fronts internes à
  chaque arrondissement. Une voie d'accès aux fichiers historiques à
  l'IRIS passerait par le CASD ou Progédo.
- **Volet qualitatif absent**. Voir §1.
- **Pas de contrôle prix / revenus**. Les données FILOSOFI (revenus
  IRIS) ne sont pas encore intégrées. Un croisement CSP × revenu × prix
  immobilier DVF serait une extension naturelle.

## 6. Reproductibilité

- Téléchargement idempotent avec checksum SHA-256 affiché à chaque fetch.
- Manifeste `data/raw/MANIFEST.md` mis à jour à la main à chaque
  source ajoutée (URL, date d'accès, taille, checksum attendu).
- Dépendances épinglées dans `pyproject.toml`.
- Chemins résolus depuis la racine du dépôt (`config.py::ROOT`), pas de
  chemins relatifs à `os.getcwd()`.

## 7. Extensions envisagées

1. **Intégration de la table de passage IRIS** (Zenodo) pour des
   comparaisons IRIS-à-IRIS rigoureuses.
2. **Tendance longue 1968-2022** à partir des séries harmonisées INSEE
   (communes), pour resituer Clerval dans un temps long.
3. **Analyse spatiale LISA / Moran local** pour identifier les clusters
   et outliers, typer les fronts pionniers et les îlots de résistance.
4. **Croisement FILOSOFI (revenus)** et **DVF (prix immobiliers)** pour
   documenter la dimension économique de la substitution.
5. **Volet Grand Paris dédié** : modélisation de la relocalisation
   (flux CSP entre 75/92 et 93/94), au-delà de la simple juxtaposition
   cartographique.
