import io
import re
import requests
import pandas as pd
import streamlit as st

st.set_page_config(page_title="PEA Universe Generator", layout="wide")
st.title("🗂 Générateur d’univers PEA (Euronext) — compatible avec ton screener")
st.caption("Télécharge le fichier Euronext, mappe les colonnes si besoin, et exporte un CSV: `ticker,exchange,country_code,name`.")

EURONEXT_URL = "https://connect.euronext.com/media/169/download"

st.subheader("1) Télécharger la source Euronext")
if st.button("📥 Télécharger le fichier Euronext (Excel)"):
    try:
        r = requests.get(EURONEXT_URL, timeout=45)
        r.raise_for_status()
        st.session_state["euronext_xls"] = r.content
        st.success("Fichier téléchargé ✅")
    except Exception as e:
        st.error(f"Erreur de téléchargement: {e}")

xls_bytes = st.session_state.get("euronext_xls")

if xls_bytes:
    # Certaines versions contiennent plusieurs feuilles : on prend la première non vide
    try:
        x = pd.ExcelFile(io.BytesIO(xls_bytes))
        df_raw = None
        for sheet in x.sheet_names:
            tmp = x.parse(sheet)
            if tmp is not None and not tmp.empty:
                df_raw = tmp
                break
        if df_raw is None or df_raw.empty:
            st.error("Le fichier Excel ne contient pas de feuilles exploitables.")
        else:
            st.success(f"Feuille utilisée: '{sheet}' — {len(df_raw)} lignes")
            st.write("Aperçu des colonnes détectées:")
            st.code(", ".join(map(str, df_raw.columns)))
            st.dataframe(df_raw.head(20), use_container_width=True)
    except Exception as e:
        st.error(f"Erreur lecture Excel (openpyxl doit être installé): {e}")
        st.stop()

    if 'df_raw' in locals() and df_raw is not None and not df_raw.empty:
        st.subheader("2) Mapper les colonnes (si l’auto-détection n’est pas parfaite)")
        cols = list(map(str, df_raw.columns))

        def guess_index(patterns, default=-1):
            """Retourne l'index de la première colonne dont le nom (lower) contient un des patterns."""
            low = [c.lower() for c in cols]
            for i, c in enumerate(low):
                for p in patterns:
                    if p in c:
                        return i
            return default

        # Heuristiques très larges
        idx_ticker  = guess_index(["mnemo","mnémo","mnemonic","ticker","symbol","code mnémo","mnemoni"])
        idx_name    = guess_index(["nom","name","issuer","company","dénomination","designation"])
        idx_exch    = guess_index(["market","exchange","place","trading","venue","compartment","segment"])
        idx_country = guess_index(["pays","country","incorporation","siège"])

        ticker_col  = st.selectbox("Colonne Ticker (obligatoire)", cols, index=idx_ticker if idx_ticker>=0 else 0)
        name_col    = st.selectbox("Colonne Nom (facultatif)", ["<aucune>"] + cols, index=(idx_name+1 if idx_name>=0 else 0))
        exch_col    = st.selectbox("Colonne Marché/Place (facultatif)", ["<aucune>"] + cols, index=(idx_exch+1 if idx_exch>=0 else 0))
        country_col = st.selectbox("Colonne Pays (facultatif)", ["<aucune>"] + cols, index=(idx_country+1 if idx_country>=0 else 0))

        st.subheader("3) Options de formatage")
        add_suffixes = st.checkbox("Ajouter automatiquement les suffixes Euronext (.PA, .AS, .BR, .LS)", value=True)

        st.subheader("4) Générer le CSV formaté")
        if st.button("🎯 Formater et télécharger pea_univers.csv"):
            # Construire le DF propre
            out = pd.DataFrame()
            out["ticker"] = df_raw[ticker_col].astype(str).str.strip()

            if name_col != "<aucune>":
                out["name"] = df_raw[name_col].astype(str).str.strip()
            else:
                out["name"] = ""

            if exch_col != "<aucune>":
                out["exchange"] = df_raw[exch_col].astype(str).str.strip()
            else:
                out["exchange"] = "Euronext"

            if country_col != "<aucune>":
                out["country_code"] = df_raw[country_col].astype(str).str.strip()
            else:
                out["country_code"] = ""

            # Ajout suffixes en fonction de la place de marché détectée
            def infer_suffix(ticker: str, exchange: str) -> str:
                if "." in ticker:
                    return ticker
                e = (exchange or "").lower()
                if "paris" in e or "france" in e:         return f"{ticker}.PA"
                if "amsterdam" in e or "netherlands" in e: return f"{ticker}.AS"
                if "brussels" in e or "belgium" in e:     return f"{ticker}.BR"
                if "lisbon" in e or "portugal" in e:      return f"{ticker}.LS"
                # Dublin moins fréquent (suffixe .IR sur Yahoo quand présent)
                if "dublin" in e or "ireland" in e:       return f"{ticker}.IR"
                return ticker

            if add_suffixes:
                out["ticker"] = [infer_suffix(t, ex) for t, ex in zip(out["ticker"], out["exchange"])]

            # Nettoyage
            out = out.dropna(subset=["ticker"])
            out = out[out["ticker"].str.len() > 0]
            out = out.drop_duplicates(subset=["ticker"])

            # Colonnes finales dans l’ordre attendu par le screener
            out = out[["ticker","exchange","country_code","name"]]

            st.success(f"✅ {len(out)} lignes prêtes pour ton screener.")
            st.dataframe(out.head(50), use_container_width=True)

            csv_bytes = out.to_csv(index=False).encode("utf-8")
            st.download_button("💾 Télécharger pea_univers.csv (format screener)",
                               data=csv_bytes, file_name="pea_univers.csv", mime="text/csv")
