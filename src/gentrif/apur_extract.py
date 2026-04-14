"""
Extraction des données CSP par quartier depuis les PDFs APUR
"Paris 1954-1999, données statistiques" (un PDF par arrondissement,
A_01.pdf à A_20.pdf).

Particularité technique : le texte des PDFs est rendu en caractères
inversés (ordre d'écriture droite-à-gauche dans le code de rendu).
pdfplumber extrait donc chaque cellule en ordre inverse — il suffit de
renverser chaque ligne caractère par caractère pour récupérer le texte.

Structure de chaque PDF (4 ou plus quartiers par arrondissement) :
- Une page d'arrondissement : "POPULATION TOTALE PAR CATÉGORIE
  SOCIOPROFESSIONNELLE (NOMENCLATURE 1982)"
- Pour chaque quartier du même arrondissement, une page homologue avec
  la même structure tabulaire.

Colonnes extraites (CSP 1982, ordre du tableau APUR) :
    Ouvriers | Employés | Professions intermédiaires | CPIS |
    Artisans-commerçants | Autres actifs | ACTIFS |
    Retraités | Enfants<15 | Autres inactifs | INACTIFS | TOTAL
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pdfplumber

from .config import DATA_RAW, QUARTIERS_PARIS

# Marqueur reversé présent sur la page CSP (nomenclature 1982)
# Correspond à "SOCIOPROFESSIONNELLE" écrit en caractères inversés.
CSP_PAGE_MARKER_REV = "ELLENNOISSEFORPOICOS"


def _rev_cell(s: str | None) -> str:
    """Inverse une chaîne complète.

    Les PDFs APUR rendent le texte entièrement en ordre droite-à-gauche
    (à la fois les caractères dans une ligne et les lignes entre elles
    dans une cellule). `s[::-1]` restaure donc l'ordre de lecture naturel
    en une seule opération.
    """
    if not s:
        return ""
    return s[::-1]


def _parse_pct_row(cell: str) -> list[float]:
    """Parse une cellule comme '5.5 2.72 8.11 8.01 0.3 4.0' (reversed)
    et retourne les 6 valeurs CSP en ordre visuel."""
    inv = _rev_cell(cell)
    # Extraire tous les nombres (décimaux signés)
    vals = [float(x) for x in re.findall(r"-?\d+\.\d+", inv)]
    return vals


def _parse_int_cell(cell: str) -> int | None:
    """Parse un effectif total (cellule du type '256 6' -> 6256)."""
    inv = _rev_cell(cell).replace(" ", "").replace("\n", "")
    if not inv or not inv.lstrip("-").isdigit():
        return None
    return int(inv)


def _find_quartier_from_header(cell: str) -> tuple[int, str] | None:
    """Depuis une cellule du type 'IRREM\\n31\\nreitrauQ\\nTNIAS'
    (reversed) retrouve (numéro, nom) du quartier.

    L'ordre des lignes après inversion n'est pas garanti (la mise en
    page rotée sépare le numéro du mot 'Quartier' sur des lignes
    distinctes). On cherche donc "Quartier" pour valider qu'il s'agit
    bien d'une page quartier (et non arrondissement), puis on prend le
    premier nombre 1-80 présent dans la cellule.
    """
    inv = _rev_cell(cell)
    if "Quartier" not in inv and "quartier" not in inv:
        return None
    m = re.search(r"\b(\d{1,2})\b", inv)
    if not m:
        return None
    num = int(m.group(1))
    if not 1 <= num <= 80:
        return None
    # Le nom est réparti sur les autres lignes ; on le reconstruit depuis
    # le mapping canonique QUARTIERS_PARIS pour éviter les ambiguïtés
    for _arr, qs in QUARTIERS_PARIS.items():
        for n, nom in qs:
            if n == num:
                return num, nom
    return None


def _is_csp_table(table: list[list[str | None]]) -> bool:
    """Une table CSP a 9 lignes (3 années × 2 lignes % / effectif + 2
    lignes évolution + 1 ligne header) et 7 colonnes, et sa dernière
    ligne (header) contient les labels CSP (Cadres, Ouvriers, etc.)."""
    if not table or len(table) < 8:
        return False
    # Inverser chaque cellule de la dernière ligne puis chercher les mots-clés
    header_text = " ".join(_rev_cell(c or "") for c in table[-1])
    return any(k in header_text for k in
               ("intellectuelles", "Cadres", "Ouvriers", "Employés"))


def extract_quartier_csp(pdf_path: Path) -> list[dict]:
    """Extrait toutes les lignes (quartier × année) d'un PDF APUR.

    Retourne une liste de dicts avec :
        num_quartier, nom, arrondissement, year,
        cpis, prof_inter, employes, ouvriers, pop_totale
    """
    records: list[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        for pi, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if CSP_PAGE_MARKER_REV not in text:
                continue
            tables = page.extract_tables() or []
            for table in tables:
                if not _is_csp_table(table):
                    continue
                quartier = _find_quartier_from_header(table[-1][0] or "")
                if quartier is None:
                    # Page arrondissement-level : on ignore ici
                    continue
                num, nom = quartier
                # Parcourir les 3 blocs année : lignes (année, %), (effectif)
                # Structure observée : 0-1 évolutions, 2-3 1999, 4-5 1990, 6-7 1982, 8 header
                year_blocks = [
                    (2, 1999),
                    (4, 1990),
                    (6, 1982),
                ]
                for row_idx, year in year_blocks:
                    if row_idx + 1 >= len(table):
                        continue
                    pct_row = table[row_idx]
                    eff_row = table[row_idx + 1]
                    # Col 2 = 6 valeurs CSP en pourcentage
                    # Dernière col de la ligne effectif = population totale
                    pcts = _parse_pct_row(pct_row[2] or "")
                    total = _parse_int_cell(eff_row[-1] or "")
                    if len(pcts) < 6 or total is None or total <= 0:
                        continue
                    # Ordre : Ouvriers, Employés, PI, CPIS, Artisans, Autres
                    ouv, emp, pi, cpis, _art, _aut = pcts[:6]
                    records.append(dict(
                        num_quartier=num,
                        nom=nom,
                        arrondissement=_arr_from_quartier(num),
                        year=year,
                        pop_totale=total,
                        pct_cpis_raw=cpis,
                        pct_prof_inter_raw=pi,
                        pct_employes_raw=emp,
                        pct_ouvriers_raw=ouv,
                        cpis=round(cpis / 100 * total),
                        prof_inter=round(pi / 100 * total),
                        employes=round(emp / 100 * total),
                        ouvriers=round(ouv / 100 * total),
                    ))
    return records


def _arr_from_quartier(num: int) -> int:
    for arr, qs in QUARTIERS_PARIS.items():
        for n, _ in qs:
            if n == num:
                return arr
    return 0


def extract_all(pdf_pattern: str = "apur_A*.pdf") -> pd.DataFrame:
    """Traite tous les PDFs APUR et retourne un DataFrame consolidé."""
    rows: list[dict] = []
    for pdf in sorted(DATA_RAW.glob(pdf_pattern)):
        try:
            recs = extract_quartier_csp(pdf)
            rows.extend(recs)
            print(f"  [ok] {pdf.name}: {len(recs)} lignes (quartier × année)")
        except Exception as e:
            print(f"  [x]  {pdf.name}: {e}")
    return pd.DataFrame(rows)


def write_wide_csv(df: pd.DataFrame, out_path: Path) -> None:
    """Pivote le DataFrame long en format wide (une ligne par quartier,
    colonnes `{ind}_{year}`) pour compatibilité avec loaders.load_historical_quartiers."""
    wide = df[["num_quartier", "nom", "arrondissement"]].drop_duplicates("num_quartier")
    for year in [1982, 1990, 1999]:
        sub = df[df["year"] == year].set_index("num_quartier")
        for col in ["cpis", "prof_inter", "employes", "ouvriers", "pop_totale"]:
            wide[f"{col}_{year}"] = wide["num_quartier"].map(sub[col])
    wide = wide.sort_values("num_quartier")
    wide.to_csv(out_path, sep=";", index=False)
    print(f"  [csv] {out_path.name}: {len(wide)} quartiers × 3 années")
