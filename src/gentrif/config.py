"""
Chemins, périmètres d'étude et constantes de référence.

Tous les chemins sont résolus relativement à la racine du dépôt, détectée
en remontant depuis l'emplacement de ce fichier. Ce choix permet d'utiliser
le package indifféremment depuis un script, un notebook, ou les tests.
"""
from __future__ import annotations

from pathlib import Path

# -- Racine du dépôt (deux niveaux au-dessus de src/gentrif/config.py)
ROOT: Path = Path(__file__).resolve().parents[2]

# -- Arborescence des données (cf. METHODOLOGY.md §3)
DATA_RAW: Path = ROOT / "data" / "raw"
DATA_INTERIM: Path = ROOT / "data" / "interim"
DATA_PROCESSED: Path = ROOT / "data" / "processed"

# -- Sorties
OUTPUT: Path = ROOT / "output"
OUT_FIGURES: Path = OUTPUT / "figures"
OUT_TABLES: Path = OUTPUT / "tables"
OUT_REPORT: Path = OUTPUT / "report"

for _d in (DATA_RAW, DATA_INTERIM, DATA_PROCESSED,
           OUT_FIGURES, OUT_TABLES, OUT_REPORT):
    _d.mkdir(parents=True, exist_ok=True)

# -- Périmètres d'analyse
DEPS_PARIS: list[str] = ["75"]
DEPS_PETITE_COURONNE: list[str] = ["92", "93", "94"]
DEPS_GRAND_PARIS: list[str] = DEPS_PARIS + DEPS_PETITE_COURONNE

SCOPES: dict[str, tuple[list[str], str]] = {
    "paris":       (DEPS_PARIS,       "Paris intra-muros"),
    "grand_paris": (DEPS_GRAND_PARIS, "Paris + petite couronne"),
}

# -- Millésimes IRIS cibles (recensement rénové, espacement 5 ans)
IRIS_YEARS: list[int] = [2007, 2012, 2017, 2022]

# -- Millésimes quartier (période Clerval, données APUR)
QUARTIER_YEARS: list[int] = [1982, 1990, 1999]

# -- Identifiants INSEE des pages de téléchargement par millésime IRIS
INSEE_PAGES: dict[int, str] = {
    2007: "2028650",
    2012: "2028582",
    2017: "4799309",
    2022: "8647014",
}

# -- 80 quartiers administratifs de Paris, nomenclature APUR
# (numéro officiel, nom, arrondissement)
QUARTIERS_PARIS: dict[int, list[tuple[int, str]]] = {
    1:  [(1, "St-Germain-l'Auxerrois"), (2, "Halles"), (3, "Palais-Royal"), (4, "Pl. Vendôme")],
    2:  [(5, "Gaillon"), (6, "Vivienne"), (7, "Mail"), (8, "Bonne-Nouvelle")],
    3:  [(9, "Arts-et-Métiers"), (10, "Enfants-Rouges"), (11, "Archives"), (12, "Ste-Avoye")],
    4:  [(13, "St-Merri"), (14, "St-Gervais"), (15, "Arsenal"), (16, "Notre-Dame")],
    5:  [(17, "St-Victor"), (18, "Jardin-des-Plantes"), (19, "Val-de-Grâce"), (20, "Sorbonne")],
    6:  [(21, "Monnaie"), (22, "Odéon"), (23, "ND-des-Champs"), (24, "St-Germain-des-Prés")],
    7:  [(25, "St-Thomas-d'Aquin"), (26, "Invalides"), (27, "École-Militaire"), (28, "Gros-Caillou")],
    8:  [(29, "Champs-Élysées"), (30, "Fbg-du-Roule"), (31, "Madeleine"), (32, "Europe")],
    9:  [(33, "St-Georges"), (34, "Chaussée-d'Antin"), (35, "Fbg-Montmartre"), (36, "Rochechouart")],
    10: [(37, "St-Vincent-de-Paul"), (38, "Porte-St-Denis"), (39, "Porte-St-Martin"), (40, "Hôp.-St-Louis")],
    11: [(41, "Folie-Méricourt"), (42, "St-Ambroise"), (43, "Roquette"), (44, "Ste-Marguerite")],
    12: [(45, "Bel-Air"), (46, "Picpus"), (47, "Bercy"), (48, "Quinze-Vingts")],
    13: [(49, "Salpêtrière"), (50, "Gare"), (51, "Maison-Blanche"), (52, "Croulebarbe")],
    14: [(53, "Montparnasse"), (54, "Parc-Montsouris"), (55, "Petit-Montrouge"), (56, "Plaisance")],
    15: [(57, "St-Lambert"), (58, "Necker"), (59, "Grenelle"), (60, "Javel")],
    16: [(61, "Auteuil"), (62, "Muette"), (63, "Porte-Dauphine"), (64, "Chaillot")],
    17: [(65, "Ternes"), (66, "Plaine-Monceaux"), (67, "Batignolles"), (68, "Épinettes")],
    18: [(69, "Grandes-Carrières"), (70, "Clignancourt"), (71, "Goutte-d'Or"), (72, "Chapelle")],
    19: [(73, "Villette"), (74, "Pont-de-Flandre"), (75, "Amérique"), (76, "Combat")],
    20: [(77, "Belleville"), (78, "St-Fargeau"), (79, "Père-Lachaise"), (80, "Charonne")],
}

# -- Catégories de synthèse pour la typologie type Clerval (cf. Fig. 6, 2010)
# Quantiles appliqués sur `ratio_gentrif` (CPIS / classes populaires)
SYNTHESIS_CATEGORIES: list[tuple[str, float, float, str]] = [
    # (label, q_low, q_high, color)
    ("Beaux quartiers",        0.95, 1.00, "#0c2340"),
    ("Gentrification achevée", 0.80, 0.95, "#1a5276"),
    ("Gentrification avancée", 0.60, 0.80, "#2e86c1"),
    ("Gentrification en cours",0.40, 0.60, "#85c1e9"),
    ("En transition",          0.20, 0.40, "#d5f5e3"),
    ("Quartiers populaires",   0.00, 0.20, "#e74c3c"),
]
