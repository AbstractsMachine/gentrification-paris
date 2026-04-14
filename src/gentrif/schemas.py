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
    "med_uc",
    "rel_med_uc",
    "poverty_rate",
    "gini",
    "d9_d1",
]


def csp_long_vars(year: int) -> dict[str, list[str]]:
    """
    Variables CSP dans les séries longues INSEE (commune, actifs
    harmonisés 1968-2022, page INSEE 1893185). Les noms varient selon
    les millésimes ; cette fonction retourne plusieurs candidats par clé.

    Clés canoniques : cpis (CS3), prof_inter (CS4), employes (CS5),
    ouvriers (CS6), pop_act (actifs totaux, univers de référence).
    """
    yy = f"{year % 100:02d}"
    return dict(
        cpis=[f"P{yy}_ACT_CS3", f"CS3_POP{yy}", f"C{yy}_ACT_CS3",
              f"CS3_ACT{yy}"],
        prof_inter=[f"P{yy}_ACT_CS4", f"CS4_POP{yy}", f"C{yy}_ACT_CS4",
                    f"CS4_ACT{yy}"],
        employes=[f"P{yy}_ACT_CS5", f"CS5_POP{yy}", f"C{yy}_ACT_CS5",
                  f"CS5_ACT{yy}"],
        ouvriers=[f"P{yy}_ACT_CS6", f"CS6_POP{yy}", f"C{yy}_ACT_CS6",
                  f"CS6_ACT{yy}"],
        pop_act=[f"P{yy}_ACT", f"ACT{yy}", f"P{yy}_POPACT"],
    )


def filosofi_vars(year: int) -> dict[str, list[str]]:
    """
    Variables canoniques FiLoSoFi IRIS (revenu fiscal disponible par UC) →
    liste de noms INSEE candidats, par ordre de préférence. Le loader prend
    la première colonne présente (les conventions varient entre millésimes).

    Clés canoniques :
        med_uc          — Médiane du niveau de vie (€ par UC)
        d1              — 1er décile du niveau de vie
        d9              — 9e décile du niveau de vie
        poverty_rate    — Taux de pauvreté (seuil 60 % médiane nationale)
        gini            — Indice de Gini
    """
    yy = f"{year % 100:02d}"
    return dict(
        med_uc=[f"DISP_MED{yy}", f"DEC_MED{yy}", f"DISP_Q2{yy}",
                f"DISP_MED_{year}"],
        d1=[f"DISP_D1{yy}", f"DEC_D1{yy}", f"DISP_D1_{year}"],
        d9=[f"DISP_D9{yy}", f"DEC_D9{yy}", f"DISP_D9_{year}"],
        poverty_rate=[f"DISP_TP60{yy}", f"TP60{yy}", f"DISP_TP60_{year}"],
        gini=[f"DISP_GI{yy}", f"DEC_GI{yy}", f"DISP_GI_{year}"],
    )
