"""
Schémas de variables CSP par millésime de recensement.

La nomenclature PCS change en 2020 (mise en œuvre dans les bases IRIS 2022).
Ce module fournit le mapping (clé canonique -> nom de variable INSEE) pour
chaque millésime, et documente la rupture de série. Voir METHODOLOGY.md §4.

Clés canoniques utilisées dans tout le pipeline :
    cpis        — Cadres et professions intellectuelles supérieures (CS3 / GSEC13)
    prof_inter  — Professions intermédiaires (CS4 / GSEC14)
    employes    — Employés (CS5 / GSEC15)
    ouvriers    — Ouvriers (CS6 / GSEC16)
    retraites   — Retraités (CS7, non reclassés en PCS 2003 ;
                  en PCS 2020, retraités reclassés par ancienne CSP fusionnés
                  avec les actifs correspondants via GSECxx_yy)
    sans_act    — Autres sans activité professionnelle
    artisans    — Artisans, commerçants, chefs d'entreprise (CS2 / GSEC12)
    pop15p      — Population de 15 ans ou plus
    pop_fr      — Population de nationalité française
    pop_etr     — Population de nationalité étrangère
"""
from __future__ import annotations


def csp_vars(year: int) -> dict[str, str | None]:
    """
    Retourne le mapping {clé canonique -> nom de variable INSEE} pour
    le millésime de recensement donné.

    Pour les millésimes <= 2021 (ancienne PCS 2003), les retraités forment
    un poste à part (CS7) non reclassé.

    Pour 2022+ (PCS 2020), les variables GSEC{XX}_{YY} agrègent actifs et
    retraités reclassés selon leur ancienne CSP. Cela rapproche la pratique
    INSEE de la méthodologie Clerval (2010) qui reclassait manuellement
    les retraités via la CSP de la personne de référence du ménage.
    """
    yy = f"{year % 100:02d}"
    if year <= 2021:
        return dict(
            cpis=f"C{yy}_POP15P_CS3",
            prof_inter=f"C{yy}_POP15P_CS4",
            employes=f"C{yy}_POP15P_CS5",
            ouvriers=f"C{yy}_POP15P_CS6",
            retraites=f"C{yy}_POP15P_CS7",
            sans_act=f"C{yy}_POP15P_CS8",
            artisans=f"C{yy}_POP15P_CS2",
            pop15p=f"P{yy}_POP15P",
            pop_fr=f"P{yy}_POP_FR",
            pop_etr=f"P{yy}_POP_ETR",
        )
    return dict(
        cpis=f"C{yy}_POP15P_STAT_GSEC13_23",
        prof_inter=f"C{yy}_POP15P_STAT_GSEC14_24",
        employes=f"C{yy}_POP15P_STAT_GSEC15_25",
        ouvriers=f"C{yy}_POP15P_STAT_GSEC16_26",
        retraites=f"C{yy}_POP15P_STAT_GSEC32",
        sans_act=f"C{yy}_POP15P_STAT_GSEC40",
        artisans=f"C{yy}_POP15P_STAT_GSEC12_22",
        pop15p=None,  # reconstruit par somme des CSP dans loaders.py
        pop_fr=f"P{yy}_POP_FR",
        pop_etr=f"P{yy}_POP_ETR",
    )


# Clés CSP utilisées pour reconstruire pop15p par sommation si absent
CSP_KEYS: list[str] = [
    "cpis", "prof_inter", "employes", "ouvriers",
    "retraites", "sans_act", "artisans",
]

# Canonical indicator names (long-format `indicator` column)
INDICATORS: list[str] = [
    "pct_cpis",
    "pct_classes_pop",
    "pct_prof_inter",
    "ratio_gentrif",
    "pct_etr",
]
