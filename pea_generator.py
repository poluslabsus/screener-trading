#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Génère une base quasi complète des actions éligibles au PEA (et PEA-PME)
Sources :
 - ProRealTime : listes France / Europe / EEE
 - Euronext : liste officielle PEA-PME (.xlsx)
 
Sortie : pea_universe.csv
"""

import re
import io
import sys
from pathlib import Path

import pandas as pd
import requests

# Vérifie openpyxl pour lire les fichiers Excel Euronext
try:
    import openpyxl  # noqa: F401
    HAVE_OPENPYXL = True
except Exception:
    HAVE_OPENPYXL = False

# --- URLs des sources ----------------------------------------------------------
PROREALTIME_URLS = {
    "prorealtime_fr": "https://www.prorealtime.com/fr/financial-instruments/actions-eligibles-pea",
    "prorealtime_eu": "https://www.prorealtime.com/fr/financial-instruments/actions-europe-eligibles-pea",
    "prorealtime_eea": "https://www.prorealtime.com/fr/financial-instruments/actions-eee-eligibles-pea",
}
EURONEXT_MEDIA = "https://connect2.euronext.com/en/media/169"

# --- Fonctions utilitaires ----------------------------------------------------
def normalize_columns(df):
    df.columns = [re.sub(r"\s+", "_", str(c).strip().lower()) for c in df.columns]
    return df

def guess_country_from_isin(isin):
    if not isinstance(isin, str) or len(isin) < 2:
        return None
    prefix = isin[:2].upper()
    mapping = {
        "FR": "France", "BE": "Belgium", "NL": "Netherlands", "DE": "Germany",
        "ES": "Spain", "IT": "Italy", "IE": "Ireland", "PT": "Portugal",
        "AT": "Austria", "FI": "Finland", "SE": "Sweden", "NO": "Norway",
        "DK": "Denmark", "LU": "Luxembourg", "GR": "Greece", "PL": "Poland",
        "CZ": "Czech Republic", "HU": "Hungary", "RO": "Romania",
        "LT": "Lithuania", "LV": "Latvia", "EE": "Estonia", "SK": "Slovakia",
        "SI": "Slovenia", "MT": "Malta", "IS": "Iceland", "LI": "Liechtenstein"
    }
    return mapping.get(prefix)

# --- Extraction ProRealTime ---------------------------------------------------
def scrape_prorealtime(url, tag):
    print(f"🔹 Lecture {tag}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(r.text)
    keep = []
    for t in tables:
        t = normalize_columns(t)
        if any("isin" in c for c in t.columns) and any("nom" in c or "name" in c for c in t.columns):
            keep.append(t)
    if not keep:
        return pd.DataFrame()
    df = pd.concat(keep, ignore_index=True)
    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if "isin" in lc:
            rename_map[c] = "isin"
        elif "mnémonique" in lc or "mnemonique" in lc or "ticker" in lc or "symbol" in lc:
            rename_map[c] = "symbol"
        elif "nom" in lc or "name" in lc:
            rename_map[c] = "name"
        elif "pays" in lc or "country" in lc:
            rename_map[c] = "country"
        elif "place" in lc or "market" in lc:
            rename_map[c] = "exchange"
    df = df.rename(columns=rename_map)
    df = df[[c for c in ["isin", "symbol", "name", "country", "exchange"] if c in df.columns]]
    df["source"] = tag
    return df

# --- Extraction Euronext PEA-PME ----------------------------------------------
def fetch_euronext_pea_pme():
    print("🔹 Recherche du fichier Euronext PEA-PME …")
    r = requests.get(EURONEXT_MEDIA, timeout=30)
    r.raise_for_status()
    xlsx_links = re.findall(r'href="([^"]+\\.xlsx[^"]*)"', r.text, flags=re.I)
    if not xlsx_links:
        print("⚠️ Aucun lien Excel trouvé sur la page Euronext.")
        return pd.DataFrame()
    link = xlsx_links[0]
    if not link.startswith("http"):
        link = "https://connect2.euronext.com" + link
    rx = requests.get(link, timeout=60)
    rx.raise_for_status()
    if not HAVE_OPENPYXL:
        print("⚠️ openpyxl non installé → fichier Euronext ignoré.")
        return pd.DataFrame()
    xls = pd.ExcelFile(io.BytesIO(rx.content), engine="openpyxl")
    sheet = xls.sheet_names[0]
    df = xls.parse(sheet)
    df = normalize_columns(df)
    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if "isin" in lc: rename_map[c] = "isin"
        elif "name" in lc or "émetteur" in lc or "issuer" in lc: rename_map[c] = "name"
        elif "symbol" in lc or "ticker" in lc: rename_map[c] = "symbol"
        elif "country" in lc or "pays" in lc: rename_map[c] = "country"
        elif "market" in lc or "place" in lc: rename_map[c] = "exchange"
    df = df.rename(columns=rename_map)
    df = df[[c for c in ["isin", "symbol", "name", "country", "exchange"] if c in df.columns]]
    df["source"] = "euronext_pea_pme"
    return df

# --- Nettoyage final -----------------------------------------------------------
def clean(df):
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    df = df.drop_duplicates(subset=["isin"], keep="first")
    if "country" in df.columns:
        df["country"] = df.apply(
            lambda r: r["country"] if r.get("country") not in ("", "nan") else guess_country_from_isin(r["isin"]),
            axis=1
        )
    return df

# --- Main ---------------------------------------------------------------------
def main():
    frames = []
    for tag, url in PROREALTIME_URLS.items():
        try:
            df = scrape_prorealtime(url, tag)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"⚠️ Erreur sur {tag}: {e}")
    try:
        pea_pme = fetch_euronext_pea_pme()
        if not pea_pme.empty:
            frames.append(pea_pme)
    except Exception as e:
        print(f"⚠️ Erreur Euronext: {e}")

    if not frames:
        print("❌ Aucune donnée collectée.")
        sys.exit(1)

    all_df = pd.concat(frames, ignore_index=True)
    all_df = clean(all_df)
    all_df["pea_eligible"] = True
    all_df["pea_pme_eligible"] = all_df["source"].eq("euronext_pea_pme")

    out_path = Path("pea_universe.csv")
    all_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"✅ {len(all_df)} lignes enregistrées dans {out_path.resolve()}")

if __name__ == "__main__":
    main()
