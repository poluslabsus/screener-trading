import streamlit as st
import pandas as pd
import io
import requests

st.set_page_config(page_title="PEA Universe Generator", layout="wide")
st.title("🗂 Générateur de liste PEA (Euronext) — Formaté pour ton screener")

st.write(
    "Cet outil télécharge la liste publique d’actions européennes (Euronext) "
    "et la transforme automatiquement en format CSV compatible avec ton screener : "
    "`ticker, exchange, country_code, name`."
)

url = "https://connect.euronext.com/media/169/download"

if st.button("📥 Télécharger et formater la liste depuis Euronext"):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content))

        # --- Recherche des colonnes utiles (souplesse face aux variations Euronext)
        cols = {c.lower(): c for c in df_raw.columns}

        ticker_col = next((cols[k] for k in cols if "mnemo" in k or "ticker" in k or "symbol" in k), None)
        name_col = next((cols[k] for k in cols if "nom" in k or "name" in k or "issuer" in k), None)
        exchange_col = next((cols[k] for k in cols if "market" in k or "exchange" in k or "place" in k), None)
        country_col = next((cols[k] for k in cols if "pays" in k or "country" in k), None)

        if not ticker_col:
            st.error("Impossible de trouver la colonne des tickers dans le fichier Euronext.")
            st.stop()

        # --- Construction du DataFrame propre
        df = pd.DataFrame()
        df["ticker"] = df_raw[ticker_col].astype(str).str.strip()
        df["name"] = df_raw[name_col].astype(str).str.strip() if name_col else ""
        df["exchange"] = df_raw[exchange_col].astype(str).str.strip() if exchange_col else "Euronext"
        df["country_code"] = df_raw[country_col].astype(str).str.upper() if country_col else ""

        # --- Ajout suffixes Euronext (.PA, .AS, .BR, .LS)
        def add_suffix(row):
            t = row["ticker"]
            e = str(row.get("exchange", "")).lower()
            if "." in t:
                return t
            if "paris" in e or "france" in e: return f"{t}.PA"
            if "amsterdam" in e or "netherlands" in e: return f"{t}.AS"
            if "brussels" in e or "belgium" in e: return f"{t}.BR"
            if "lisbon" in e or "portugal" in e: return f"{t}.LS"
            return t

        df["ticker"] = df.apply(add_suffix, axis=1)

        # --- Nettoyage et déduplication
        df = df.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])
        df = df[["ticker", "exchange", "country_code", "name"]]

        st.success(f"✅ Univers PEA formaté : {len(df)} actions prêtes à l'emploi.")
        st.dataframe(df.head(50), use_container_width=True)

        # --- Téléchargement du CSV
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "💾 Télécharger pea_univers.csv (format screener)",
            data=csv_bytes,
            file_name="pea_univers.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Erreur : {e}")
        st.info("Si la structure du fichier Euronext change, réessaie plus tard.")
