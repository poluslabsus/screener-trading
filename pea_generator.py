import streamlit as st, pandas as pd, requests, io

st.set_page_config(page_title="PEA Generator", layout="wide")
st.title("🗂 Générateur PEA (Euronext)")
st.caption("Télécharge automatiquement la liste publique Euronext PEA, la nettoie et exporte un CSV compatible avec le screener.")

url = "https://connect.euronext.com/media/169/download"

if st.button("📥 Générer pea_univers.csv"):
    try:
        r = requests.get(url, timeout=40)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))

        # Repérage automatique des colonnes utiles
        col_ticker = next((c for c in df.columns if "mnemo" in c.lower() or "ticker" in c.lower()), None)
        col_name   = next((c for c in df.columns if "nom" in c.lower() or "name" in c.lower()), None)
        col_market = next((c for c in df.columns if "market" in c.lower() or "march" in c.lower()), None)
        col_country= next((c for c in df.columns if "pays" in c.lower() or "country" in c.lower()), None)

        if not col_ticker:
            st.error("Impossible d'identifier la colonne des tickers (mnemonic).")
            st.stop()

        df_out = pd.DataFrame({
            "ticker": df[col_ticker].astype(str).str.strip(),
            "name":   df[col_name].astype(str).str.strip() if col_name else "",
            "exchange": df[col_market].astype(str).str.strip() if col_market else "Euronext",
            "country_code": df[col_country].astype(str).str.upper() if col_country else ""
        })

        # Ajout suffixes Yahoo
        def add_suffix(t, e):
            e = e.lower()
            if "." in t: return t
            if "paris" in e or "france" in e: return f"{t}.PA"
            if "amsterdam" in e or "netherlands" in e: return f"{t}.AS"
            if "brussels" in e or "belgium" in e: return f"{t}.BR"
            if "lisbon" in e or "portugal" in e: return f"{t}.LS"
            if "dublin" in e or "ireland" in e: return f"{t}.IR"
            return t

        df_out["ticker"] = [add_suffix(t, e) for t, e in zip(df_out["ticker"], df_out["exchange"])]

        df_out = df_out.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])
        st.success(f"✅ {len(df_out)} lignes détectées.")
        st.dataframe(df_out.head(50), use_container_width=True)

        csv_bytes = df_out.to_csv(index=False).encode("utf-8")
        st.download_button("💾 Télécharger pea_univers.csv", csv_bytes, "pea_univers.csv", "text/csv")

    except Exception as e:
        st.error(f"Erreur : {e}")
