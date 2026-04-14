"""
Téléchargement des sources primaires (INSEE, APUR, opendata).

Principes :
- Toute source va dans `data/raw/` et n'est jamais modifiée sur place.
- Un téléchargement déjà présent est réutilisé (cache implicite).
- La provenance est documentée dans `data/raw/MANIFEST.md`.
"""
from __future__ import annotations

import hashlib
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
              f"(cf. MANIFEST.md)")
        return None

    for base in [f"BASE_TD_FILO_DISP_IRIS_{year}",
                 f"base-ic-disp-menages-{year}",
                 f"base-cc-filosofi-{year}"]:
        for ext in ["_xlsx.zip", ".zip", "_csv.zip"]:
            url = f"https://www.insee.fr/fr/statistiques/fichier/{pid}/{base}{ext}"
            zp = DATA_RAW / f"filosofi_{year}.zip"
            try:
                r = requests.get(url, timeout=180)
                if r.status_code == 200 and len(r.content) > 10_000:
                    zp.write_bytes(r.content)
                    with zipfile.ZipFile(zp) as zf:
                        cands = sorted(
                            [n for n in zf.namelist()
                             if n.endswith((".csv", ".xls", ".xlsx"))],
                            key=lambda n: zf.getinfo(n).file_size, reverse=True,
                        )
                        if cands:
                            zf.extract(cands[0], DATA_RAW)
                            out = DATA_RAW / cands[0]
                            print(f"  [ok] FiLoSoFi {year}: extrait {cands[0]}")
                            return out
            except Exception:
                continue
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

    pid = INSEE_PAGE_LONG_SERIES
    for name in ["base-cc-serie-historique",
                 "base-cc-serie-longue-1968",
                 "base-cc-evol-struct-pop-1968-2022"]:
        for ext in [".xlsx", "_xlsx.zip", ".zip"]:
            url = f"https://www.insee.fr/fr/statistiques/fichier/{pid}/{name}{ext}"
            zp = DATA_RAW / f"long_series{ext if ext.endswith('.xlsx') else '.zip'}"
            try:
                r = requests.get(url, timeout=180)
                if r.status_code == 200 and len(r.content) > 10_000:
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
                    else:
                        print(f"  [ok] séries longues : {zp.name}")
                        return zp
            except Exception:
                continue
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
