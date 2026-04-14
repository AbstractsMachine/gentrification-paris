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

from .config import DATA_RAW, INSEE_PAGES


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

    # 2. Téléchargement via URLs INSEE standardisées
    for base in [f"base-ic-pop-{year}", f"base-ic-evol-struct-pop-{year}"]:
        for ext in ["_csv.zip", ".zip", "_xlsx.zip"]:
            url = f"https://www.insee.fr/fr/statistiques/fichier/{pid}/{base}{ext}"
            zp = DATA_RAW / f"iris_{year}.zip"
            try:
                r = requests.get(url, timeout=120)
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
            except Exception:
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


def fetch_apur_pdf() -> Path | None:
    """Recueil APUR Paris 1954-1999 (PDF)."""
    pdf = DATA_RAW / "apur_paris_1954_1999.pdf"
    fetch("https://www.apur.org/sites/default/files/documents/paris.pdf",
          pdf, "PDF APUR Paris 1954-1999")
    return pdf if pdf.exists() else None
