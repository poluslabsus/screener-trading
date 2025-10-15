#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Version allégée : récupère la liste principale des actions éligibles PEA
(France + Europe + EEE depuis ProRealTime, plus la liste PEA-PME d'Euronext)
et exporte seulement les colonnes ISIN + Name.

Sortie : pea_light.csv
"""

import re
import io
import sys
import pandas as pd
import requests
from pathlib import Path

# --- URLs principales ---
PROREALTIME_URLS = [
    "https://www.prorealtime.com/fr/financial-instruments/actions-eligibles-pea",
    "https://www.prorealtime.com/fr/financial-instruments/actions-europe-eligibles-pea",
    "https://www.prorealtime.com/fr/financial-instruments/actions-eee-eligibles-pea",
]
EURONEXT_PAGE = "https://connect2.euronext.com/en/media/169"

# --- Extraction ProRealTime ---
def get_prorealtime_tables():
    frames = []
    for url in PROREALTIME_URLS:
        print(f"🔹 Lecture : {url}")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            tables = pd.read_html(r.text)
            for t in tables:
                t.columns = [c.lower().strip() for c in t.columns]
                if any("isin" in c for c in t.columns) and any("nom" in c or "name" in c for c in t.columns):
                    rename = {}
                    for c in t.columns:
                        if "isin" in c: rename[c] = "isin"
                        if "nom" in c or "name" in c: rename[c] = "name"
                    df = t.rename(columns=rename)
                    df = df[["isin", "name"]].dropna()
                    frames.append(df)
        except Exception as e:
            print(f"⚠️ Erreur ProRealTime : {e}")
    return frames

# --- Extraction Euronext PEA-PME ---
def get_euronext_pea_pme():
    print("🔹 Lecture Euronext PEA-PME")
    try:
        r = requests.get(EURONEXT_PAGE, timeout=30)
        r.raise_for_status()
        xlsx_links = re.findall(r'href="([^"]+\\.xlsx[^"]*)"', r.text, flags=re.I)
        if not xlsx_links:
            print("⚠️ Aucun lien XLSX trouvé.")
            return pd.DataFrame()
        link = xlsx_links[0]
        if not link.startswith("http"):
            link = "https://connect2.euronext.com" + link
        rx = requests.get(link, timeout=60)
        rx.raise_for_status()
        import openpyxl  # nécessaire pour Excel
        xls = pd.ExcelFile(io.BytesIO(rx.content), engine="openpyxl")
        sheet = xls.sheet_names[0]
        df = xls.parse(sheet)
        df.columns = [c.lower().strip() for c in df.columns]
        rename = {}
        for c in df.columns:
            if "isin" in c: rename[c] = "isin"
            if "name" in c or "émetteur" in c or "issuer" in c: rename[c] = "name"
        df = df.rename(columns=rename)
        return df[["isin", "name"]].dropna()
    except Exception as e:
        print(f"⚠️ Erreur Euronext : {e}")
        return pd.DataFrame()

# --- Assemblage ---
def main():
    frames = get_prorealtime_tables()
    pea_pme = get_euronext_pea_pme()
    if not pea_pme.empty:
        frames.append(pea_pme)
    if not frames:
        print("❌ Aucune donnée récupérée.")
        sys.exit(1)
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["isin"])
    df.to_csv("pea_light.csv", index=False, encoding="utf-8")
    print(f"✅ {len(df)} lignes enregistrées dans pea_light.csv")

if __name__ == "__main__":
    main()
