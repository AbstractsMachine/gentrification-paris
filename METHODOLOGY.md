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

## 2bis. État vs processus : typologie trajectoire 2×2

La gentrification désigne un **processus** de substitution sociale dans le
temps, pas un **état** social à une date donnée. Cette distinction est
cardinale pour l'interprétation cartographique : un niveau élevé du
`ratio_gentrif` à une date donnée n'est pas un indicateur de
gentrification. Neuilly-sur-Seine et le 16e arrondissement présentent un
ratio très élevé depuis des décennies — ce sont des **beaux quartiers
constitués**, pas des fronts pionniers en voie de gentrification.

Deux classifications distinctes sont donc retenues :

### Classification en niveau (`classify_level`, `plot_level_typology`)

Répartition des unités spatiales à une **date donnée** en six classes de
géographie sociale (quantiles du `ratio_gentrif`). Décrit un *état*. Utile
pour localiser les beaux quartiers et les quartiers populaires, mais ne
distingue pas un quartier historiquement aisé d'un quartier récemment
gentrifié. Carte à placer en annexe.

### Classification en trajectoire (`classify_trajectory`, `plot_trajectory`)

Typologie 2×2 croisant le **niveau initial** (à t0) avec l'**évolution**
du ratio entre t0 et t1 :

| Niveau initial | Évolution | Classe                      |
|----------------|-----------|-----------------------------|
| bas            | hausse    | **Gentrification**          |
| bas            | stable/baisse | **Relégation**           |
| haut           | hausse    | **Consolidation bourgeoise**|
| haut           | baisse    | **Déclassement**            |

Le seuil de niveau par défaut est la médiane du `ratio_gentrif` à t0 sur
le périmètre considéré ; le seuil de variation par défaut est 0 (toute
hausse strictement positive classe en *hausse*). Ces paramètres sont
exposés par la fonction pour permettre des analyses de sensibilité.

Cette classification prolonge l'intuition de Clerval (2010, Fig. 6) —
les *Beaux quartiers* y sont déjà traités comme un point de départ
structurel, distinct du *front pionnier* qui désigne les quartiers
basculant au cours de la période. Elle formalise mécaniquement ce que
Clerval lisait qualitativement à partir des cartes successives.

Carte de référence pour l'analyse de la gentrification dans le projet.

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

### 4.3. Séries de revenus FiLoSoFi / RFL

Le dispositif FiLoSoFi (*Fichier localisé social et fiscal*) remplace à
partir de 2012 les RFL (*Revenus fiscaux localisés*, 2001-2012) et fournit
par IRIS la médiane du niveau de vie, les déciles D1/D9, le taux de
pauvreté (seuil 60 % de la médiane nationale) et l'indice de Gini.

Ruptures identifiées :

- **Rupture 2001-2012 → 2012+ (RFL → FiLoSoFi)** : changement d'univers
  fiscal (passage du revenu fiscal déclaré au revenu disponible après
  prestations et prélèvements), non strictement comparable. Ce projet
  démarre les séries de revenus **à partir de 2012** pour éviter ce biais.
- **Évolutions méthodologiques 2018-2019** : modification du traitement
  des prestations sociales et des impôts (cf. notes méthodologiques
  INSEE). Les séries FiLoSoFi 2012→2017 et 2019+ sont strictement
  comparables entre elles, mais la comparaison exacte 2017→2019 demande
  prudence.

Métrique retenue : `rel_med_uc = med_uc(i) / médiane(med_uc, périmètre)`
— revenu médian relatif au périmètre analysé. Par construction centré
sur 1.0, ce qui absorbe l'inflation générale des revenus et isole le
*positionnement social relatif* de l'IRIS. La **trajectoire 2×2** y
s'applique directement (cf. §2bis) : seuil de niveau = médiane du rel_med_uc
à t0, seuil d'évolution = 0.

### 4.4. Zonages IRIS évolutifs

Les codes IRIS sont révisés périodiquement (refontes 2008, 2015…). Une
comparaison IRIS-à-IRIS 2007 vs 2022 est donc approximative pour les
territoires redécoupés. Le dépôt Zenodo *"Harmonized INSEE
socio-demographic IRIS-level data and IRIS conversion file (2010-2020)"*
fournit une table de passage intégrée dans le pipeline :

- Schéma attendu (normalisé par `harmonize._normalise_crosswalk_cols`) :
  `(iris_src, iris_dst, weight)`, éventuellement `(year_src, year_dst)`.
- Application : `apply_crosswalk_wide(df, crosswalk, count_cols=[...])`
  agrège les effectifs bruts par IRIS cible avec pondération par `weight`,
  puis recalcule les indicateurs via `compute_indicators`. Appliqué
  automatiquement dans `scripts/build_processed.build_iris_long` quand
  `data/raw/iris_crosswalk.csv` est présent.
- Fallback identité : un IRIS non référencé dans le crosswalk est
  conservé tel quel (`iris_src = iris_dst`, `weight = 1.0`).

Les ratios et parts (`pct_*`, `ratio_gentrif`, `rel_med_uc`) ne sont **pas**
additifs — ils sont toujours recalculés après agrégation pondérée des
effectifs, jamais moyennés. Les analyses en format long gardent un pointeur
`geo_code_harmonised` vers l'IRIS cible pour jointure cartographique, sans
ré-agrégation.

Sans crosswalk disponible, l'essentiel des comparaisons se lit à
l'**échelle du quartier perceptible** (grappe d'IRIS contigus), ce qui
relativise l'impact des refontes ponctuelles.

### 4.5. Séries harmonisées INSEE 1968-2022 (communes)

Pour raccorder la période Clerval (APUR, 1982-1999, quartiers parisiens)
à la période IRIS (2007-2022, infracommunal), le projet mobilise les
**séries harmonisées** publiées par l'INSEE (page 1893185). Ces données
fournissent, pour les **communes** de France métropolitaine et à
nomenclature stabilisée, la structure socio-professionnelle des actifs
aux points de recensement 1968, 1975, 1982, 1990, 1999, 2006, 2011, 2016,
2021.

Choix retenus :

- **Univers de référence** : actifs ayant un emploi (variable `P{YY}_ACT`
  ou équivalent). C'est le seul univers strictement comparable sur la
  période, les retraités et inactifs n'étant pas classifiés de manière
  homogène en 1968 comme en 2021.
- **Résolution spatiale** : communes entières. Paris apparaît comme une
  seule commune (75056) dans la plupart des fichiers INSEE — pour le
  grain arrondissement, il faut utiliser les IRIS (à partir de 2007) ou
  les 80 quartiers APUR (1982-1999).
- **Métrique** : `ratio_gentrif` calculé de la même manière qu'aux autres
  échelles (cf. §2). La trajectoire 2×2 (§2bis) s'applique sans
  modification, avec t0 = 1968 et t1 = 2021 (ou sous-périodes choisies).
- **Ruptures internes aux séries longues** : l'INSEE signale quelques
  révisions mineures de nomenclature entre 1982 et 1999 (réaménagement
  PCS-1982 → PCS-2003), absorbées par l'harmonisation rétrospective.
  La série est considérée exploitable telle quelle pour la lecture
  structurelle visée par le projet.

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
- **Contrôle par les revenus (FiLoSoFi) intégré mais partiel**. La couche
  revenus (cf. §4.3) est en place (`schemas.filosofi_vars`,
  `loaders.load_filosofi_iris`, `compute_income_indicators`). Elle permet
  la triangulation CSP × revenus via la typologie trajectoire (§2bis)
  appliquée indifféremment à `ratio_gentrif` et `rel_med_uc`. En revanche,
  les prix immobiliers DVF restent à intégrer pour fermer le triangle
  CSP × revenu × prix (cf. §7).

## 6. Reproductibilité

- Téléchargement idempotent avec checksum SHA-256 affiché à chaque fetch.
- Manifeste `data/raw/MANIFEST.md` mis à jour à la main à chaque
  source ajoutée (URL, date d'accès, taille, checksum attendu).
- Dépendances épinglées dans `pyproject.toml`.
- Chemins résolus depuis la racine du dépôt (`config.py::ROOT`), pas de
  chemins relatifs à `os.getcwd()`.

## 7. Extensions envisagées

**En place :**

- ✅ **Typologie trajectoire 2×2** (§2bis) — `classify_trajectory`,
  `plot_trajectory`. Carte de référence pour caractériser le processus
  de gentrification, indépendamment de l'état social à une date donnée.
- ✅ **FiLoSoFi (revenus IRIS)** (§4.3) — `filosofi_vars`,
  `load_filosofi_iris`, `compute_income_indicators`. Triangulation
  CSP × revenus via application de la trajectoire 2×2 à `rel_med_uc`.
- ✅ **Table de passage IRIS** (§4.4) — `harmonize.load_iris_crosswalk`,
  `apply_crosswalk_wide`. Harmonisation additive des effectifs sur le
  zonage IRIS cible ; applique l'identité si aucun crosswalk n'est
  présent, ne modifie donc pas le comportement existant.
- ✅ **Tendance longue 1968-2022** (§4.5) — `schemas.csp_long_vars`,
  `loaders.load_long_series`. Séries harmonisées INSEE au niveau
  commune, ratio_gentrif calculable de 1968 à 2021. Raccord possible
  avec la période APUR (1982-1999, quartiers) et IRIS (2007-2022).

**Restant à faire :**

1. **Analyse spatiale LISA / Moran local** pour identifier clusters,
   outliers, fronts pionniers et îlots de résistance. Complément formel
   à la lecture visuelle des cartes trajectoire.
2. **DVF (prix immobiliers)** pour documenter la dimension économique de
   la substitution. DVF est en open data depuis 2014 au niveau
   transaction — agrégation à l'IRIS à construire.
3. **Diplômes** comme troisième axe de triangulation (CSP × revenu ×
   diplôme) via les variables `P{YY}_NSCOL15P_*` INSEE.
4. **Volet Grand Paris dédié** : modélisation de la relocalisation
   (flux CSP entre 75/92 et 93/94), au-delà de la simple juxtaposition
   cartographique.
