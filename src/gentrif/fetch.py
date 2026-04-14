"""
Téléchargement des sources primaires (INSEE, APUR, opendata).

Principes :
- Toute source va dans `data/raw/` et n'est jamais modifiée sur place.
- Un téléchargement déjà présent est réutilisé (cache implicite).
- La provenance est documentée dans `data/raw/MANIFEST.md`.
"""
from __future__ import annotations

import hashlib
import re
import zipfile
from pathlib import Path

import requests

from .config import (
    DATA_RAW,
    INSEE_PAGE_LONG_SERIES,
    INSEE_PAGES,
    INSEE_PAGES_FILOSOFI,
    IRIS_CROSSWALK_FILENAME,
    IRIS_CROSSWALK_URL,
    LONG_SERIES_FILENAME,
)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(url: str, dest: Path, desc: str = "") -> bool:
    """Télécharge `url` vers `dest`. Ne fait rien si `dest` existe."""
    if dest.exists():
        print(f"  [ok] {desc or dest.name} (cache)")
        return True
    print(f"  [dl] {desc or url[:80]}...")
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(r.content)
        print(f"       {len(r.content)/1024/1024:.1f} Mo — sha256={sha256(dest)[:12]}")
        return True
    except Exception as e:
        print(f"  [x]  {e}")
        return False


def scrape_insee_downloads(page_id: str,
                           extensions=(".zip", ".xlsx", ".xls", ".csv")
                           ) -> list[str]:
    """Scrape une page INSEE et retourne les URLs de fichiers téléchargeables."""
    try:
        r = requests.get(f"https://www.insee.fr/fr/statistiques/{page_id}",
                         timeout=30)
        if r.status_code != 200:
            return []
    except Exception:
        return []
    ext_re = "|".join(re.escape(e) for e in extensions)
    urls = re.findall(
        rf'href="(/fr/statistiques/fichier/{page_id}/[^"]+(?:{ext_re}))"',
        r.text,
    )
    return ["https://www.insee.fr" + u for u in urls]


def fetch_iris_year(year: int) -> Path | None:
    """
    Récupère la base IRIS Population pour un millésime donné.
    Tente plusieurs conventions de nom et extensions successivement.
    """
    # 1. Fichier déjà présent ?
    for pat in [f"*pop*{year}*.csv", f"*{year}*pop*.csv", f"*{year}*.csv",
                f"*pop*{year}*.xls*", f"*{year}*.xls*"]:
        hits = sorted(DATA_RAW.glob(pat), key=lambda p: p.stat().st_size, reverse=True)
        if hits:
            print(f"  [ok] IRIS {year}: {hits[0].name}")
            return hits[0]

    pid = INSEE_PAGES.get(year)
    if not pid:
        return None

    # 2. Téléchargement via URLs INSEE.
    # Les millésimes anciens (2007, 2012) ne suivent pas la convention
    # `base-ic-{pop|evol-struct-pop}-{year}` des millésimes récents.
    SPECIAL_URLS = {
        2007: f"https://www.insee.fr/fr/statistiques/fichier/{pid}/BTX_IC_POP_2007.zip",
        2012: f"https://www.insee.fr/fr/statistiques/fichier/{pid}/infra-population-2012.zip",
    }
    urls_to_try: list[str] = []
    if year in SPECIAL_URLS:
        urls_to_try.append(SPECIAL_URLS[year])
    for base in [f"base-ic-pop-{year}", f"base-ic-evol-struct-pop-{year}"]:
        for ext in ["_csv.zip", ".zip", "_xlsx.zip"]:
            urls_to_try.append(
                f"https://www.insee.fr/fr/statistiques/fichier/{pid}/{base}{ext}"
            )

    for url in urls_to_try:
        zp = DATA_RAW / f"iris_{year}.zip"
        try:
            r = requests.get(url, timeout=180)
            if r.status_code == 200 and len(r.content) > 10_000:
                zp.write_bytes(r.content)
                with zipfile.ZipFile(zp) as zf:
                    candidates = sorted(
                        [n for n in zf.namelist() if n.endswith((".csv", ".xls", ".xlsx"))],
                        key=lambda n: zf.getinfo(n).file_size, reverse=True,
                    )
                    if candidates:
                        zf.extract(candidates[0], DATA_RAW)
                        out = DATA_RAW / candidates[0]
                        print(f"  [ok] IRIS {year}: extrait {candidates[0]}")
                        return out
        except Exception as e:
            print(f"  [..] {year} {url.split('/')[-1]}: {e.__class__.__name__}")
            continue
    return None


def fetch_iris_contours(dep_codes: list[str]) -> Path | None:
    """Contours GeoJSON des IRIS pour une liste de départements."""
    cache = DATA_RAW / f"iris_contours_{'_'.join(dep_codes)}.geojson"
    if not cache.exists():
        filt = " OR ".join([f'dep_code="{d}"' for d in dep_codes])
        url = (
            "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            f"georef-france-iris/exports/geojson?where={filt}&limit=-1"
        )
        fetch(url, cache, f"Contours IRIS dép.{','.join(dep_codes)}")
    return cache if cache.exists() else None


def fetch_quartier_contours() -> Path | None:
    """Contours GeoJSON des 80 quartiers administratifs de Paris."""
    cache = DATA_RAW / "quartiers_paris.geojson"
    if not cache.exists():
        url = (
            "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
            "quartier_paris/exports/geojson?limit=-1"
        )
        fetch(url, cache, "Contours 80 quartiers")
    return cache if cache.exists() else None


def fetch_filosofi_year(year: int) -> Path | None:
    """
    Récupère la base FiLoSoFi IRIS (revenus fiscaux localisés disponibles)
    pour un millésime donné. Fonctionne en cache-first ; si rien n'est
    trouvé et qu'une page INSEE est renseignée dans INSEE_PAGES_FILOSOFI,
    tente un téléchargement opportuniste.

    Voir METHODOLOGY.md §4.3 pour la rupture RFL → FiLoSoFi en 2012 et les
    évolutions méthodologiques de 2018-2019.
    """
    for pat in [f"*FILO*DISP*IRIS*{year}*.xls*",
                f"*filosofi*{year}*iris*.xls*",
                f"*FILO*{year}*.xls*",
                f"*filosofi*{year}*.csv",
                f"*FILO*{year}*.csv"]:
        hits = sorted(DATA_RAW.glob(pat),
                      key=lambda p: p.stat().st_size, reverse=True)
        if hits:
            print(f"  [ok] FiLoSoFi {year}: {hits[0].name}")
            return hits[0]

    pid = INSEE_PAGES_FILOSOFI.get(year)
    if not pid:
        print(f"  [..] FiLoSoFi {year}: page INSEE non renseignée "
              f"(cf. config.INSEE_PAGES_FILOSOFI)")
        return None

    # Scrape la page INSEE pour trouver les URLs de téléchargement réels.
    # Préfère le revenu *disponible* (DISP) au revenu *déclaré* (DEC) — cf.
    # METHODOLOGY §4.3. Le format XLSX prime sur CSV pour simplicité de parse.
    downloads = scrape_insee_downloads(pid)
    if not downloads:
        print(f"  [x]  FiLoSoFi {year}: page {pid} inaccessible")
        return None

    def priority(url: str) -> tuple:
        u = url.upper()
        # score : plus bas = prioritaire
        iris = 0 if "IRIS" in u else 1
        disp = 0 if "DISP" in u else 1
        # "XLSX" OU typo INSEE "XSLX"
        xl = 0 if ("XLSX" in u or "XSLX" in u) else 1
        return (iris, disp, xl)

    downloads.sort(key=priority)
    chosen = downloads[0]
    print(f"  [dl] FiLoSoFi {year} <- {chosen.split('/')[-1]}")
    zp = DATA_RAW / f"filosofi_{year}_{chosen.split('/')[-1]}"
    try:
        r = requests.get(chosen, timeout=180)
        if r.status_code != 200 or len(r.content) < 10_000:
            print(f"  [x]  téléchargement échoué ({r.status_code})")
            return None
        zp.write_bytes(r.content)
        if zp.suffix == ".zip":
            with zipfile.ZipFile(zp) as zf:
                cands = sorted(
                    [n for n in zf.namelist()
                     if n.endswith((".csv", ".xls", ".xlsx"))],
                    key=lambda n: zf.getinfo(n).file_size, reverse=True,
                )
                if cands:
                    zf.extract(cands[0], DATA_RAW)
                    out = DATA_RAW / cands[0]
                    print(f"  [ok] FiLoSoFi {year}: extrait {cands[0]} "
                          f"({len(r.content)/1024/1024:.1f} Mo zip)")
                    return out
        print(f"  [ok] FiLoSoFi {year}: {zp.name}")
        return zp
    except Exception as e:
        print(f"  [x]  FiLoSoFi {year}: {e.__class__.__name__}")
        return None


def fetch_iris_crosswalk() -> Path | None:
    """
    Récupère la table de passage IRIS inter-millésimes (Zenodo).

    Cache-first sur `data/raw/iris_crosswalk.csv`. Si absent et que
    `IRIS_CROSSWALK_URL` est renseignée, tente un DL direct. Sinon, le
    chercheur doit la placer manuellement (cf. MANIFEST.md §Tables de passage).
    """
    dest = DATA_RAW / IRIS_CROSSWALK_FILENAME
    if dest.exists():
        print(f"  [ok] crosswalk IRIS (cache: {dest.name})")
        return dest
    if not IRIS_CROSSWALK_URL:
        print("  [..] crosswalk IRIS : URL non renseignée "
              "(cf. config.IRIS_CROSSWALK_URL ou fichier manuel)")
        return None
    fetch(IRIS_CROSSWALK_URL, dest, "Table de passage IRIS (Zenodo)")
    return dest if dest.exists() else None


def fetch_long_series() -> Path | None:
    """
    Récupère la base commune/arrondissement des séries harmonisées 1968-2022
    (INSEE page 1893185). Cache-first, fallback DL opportuniste.
    """
    for pat in ["*serie*historique*.xls*", "*evol*struct*pop*1968*.xls*",
                "*serie*longue*.xls*", "*1968*2022*.xls*"]:
        hits = sorted(DATA_RAW.glob(pat),
                      key=lambda p: p.stat().st_size, reverse=True)
        if hits:
            print(f"  [ok] séries longues: {hits[0].name}")
            return hits[0]

    # La page INSEE 1893185 expose plusieurs fichiers (csp, csp×dipl,
    # emploi×sexe…). On veut "pop-act2554-csp-cd-{68-22}.zip" (population
    # active 25-54 ans × CSP, niveau commune-département 1968-2022).
    downloads = scrape_insee_downloads(INSEE_PAGE_LONG_SERIES)
    if not downloads:
        print(f"  [x]  séries longues : page INSEE inaccessible")
        return None

    def priority(url: str) -> tuple:
        u = url.lower()
        csp = 0 if "csp" in u and "dipl" not in u and "sa" not in u else 1
        has_cd = 0 if "-cd-" in u else 1
        return (csp, has_cd)

    downloads.sort(key=priority)
    chosen = downloads[0]
    print(f"  [dl] séries longues <- {chosen.split('/')[-1]}")
    zp = DATA_RAW / chosen.split("/")[-1]
    try:
        r = requests.get(chosen, timeout=180)
        if r.status_code != 200 or len(r.content) < 10_000:
            return None
        zp.write_bytes(r.content)
        if zp.suffix == ".zip":
            with zipfile.ZipFile(zp) as zf:
                cands = sorted(
                    [n for n in zf.namelist()
                     if n.endswith((".xls", ".xlsx", ".csv"))],
                    key=lambda n: zf.getinfo(n).file_size, reverse=True,
                )
                if cands:
                    zf.extract(cands[0], DATA_RAW)
                    out = DATA_RAW / cands[0]
                    print(f"  [ok] séries longues : extrait {cands[0]}")
                    return out
        return zp
    except Exception as e:
        print(f"  [x]  séries longues : {e.__class__.__name__}")
        return None


def fetch_commune_contours(dep_codes: list[str]) -> Path | None:
    """Contours GeoJSON des communes pour un périmètre départemental."""
    cache = DATA_RAW / f"communes_contours_{'_'.join(dep_codes)}.geojson"
    if not cache.exists():
        filt = " OR ".join([f'dep_code="{d}"' for d in dep_codes])
        url = (
            "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            f"georef-france-commune/exports/geojson?where={filt}&limit=-1"
        )
        fetch(url, cache, f"Contours communes dép.{','.join(dep_codes)}")
    return cache if cache.exists() else None


def fetch_apur_pdf() -> Path | None:
    """Recueil APUR Paris 1954-1999 (PDF)."""
    pdf = DATA_RAW / "apur_paris_1954_1999.pdf"
    fetch("https://www.apur.org/sites/default/files/documents/paris.pdf",
          pdf, "PDF APUR Paris 1954-1999")
    return pdf if pdf.exists() else None
