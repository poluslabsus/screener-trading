import io
import re
import requests
import pandas as pd
import streamlit as st

st.set_page_config(page_title="PEA Universe Generator", layout="wide")
st.title("🗂 Générateur d’univers PEA (Euronext) — compatible avec le screener")
st.caption("Télécharge la source Euronext, choisis les colonnes si besoin, ajoute les suffixes Yahoo, et exporte un CSV prêt pour l’app (`ticker,exchange,country_code,name`).")

EURONEXT_URL = "https://connect.euronext.com/media/169/download"

# ---------- 1) Télécharger le fichier source ----------
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

def read_any_sheet(xls_content: bytes) -> pd.DataFrame:
    """Lit toutes les feuilles, les concatène, garde seulement les colonnes non vides."""
    x = pd.ExcelFile(io.BytesIO(xls_content))
    frames = []
    for sheet in x.sheet_names:
        try:
            df = x.parse(sheet)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    # Concatène en normalisant les noms de colonnes (si identiques)
    # On garde toutes les colonnes, puis l'utilisateur choisira lesquelles utiliser
    big = pd.concat(frames, ignore_index=True, sort=False)
    return big

def find_column_idx(cols, patterns):
    low = [str(c).lower() for c in cols]
    for i, c in enumerate(low):
        for p in patterns:
            if p in c:
                return i
    return -1

if xls_bytes:
    # ---------- 2) Lecture Excel & aperçu ----------
    try:
        df_raw = read_any_sheet(xls_bytes)
        if df_raw.empty:
            st.error("Le fichier Excel ne contient pas de données exploitables.")
            st.stop()
        st.success(f"Source chargée : {len(df_raw)} lignes au total.")
        st.write("Colonnes détectées :")
        st.code(", ".join(map(str, df_raw.columns)))
        st.dataframe(df_raw.head(20), use_container_width=True)
    except Exception as e:
        st.error(f"Erreur lecture Excel (pense à installer openpyxl) : {e}")
        st.stop()

    # ---------- 3) Mapping des colonnes ----------
    st.subheader("2) Mapper les colonnes")
    cols = list(df_raw.columns)

    # Heuristiques très larges (inclut accents et variantes)
    idx_ticker  = find_column_idx(cols, ["mnemo", "mnémo", "mnemonic", "ticker", "symbol", "code mnémo", "mnemoni"])
    idx_name    = find_column_idx(cols, ["nom", "name", "issuer", "company", "dénomination", "designation"])
    idx_exch    = find_column_idx(cols, ["market", "exchange", "place", "trading", "venue", "compartment", "segment"])
    idx_country = find_column_idx(cols, ["pays", "country", "incorporation", "siège", "head office"])

    ticker_col  = st.selectbox("Colonne Ticker (obligatoire)", cols, index=max(idx_ticker,0))
    name_col    = st.selectbox("Colonne Nom (facultatif)", ["<aucune>"] + cols, index=(idx_name+1 if idx_name>=0 else 0))
    exch_col    = st.selectbox("Colonne Marché/Place (facultatif)", ["<aucune>"] + cols, index=(idx_exch+1 if idx_exch>=0 else 0))
    country_col = st.selectbox("Colonne Pays (facultatif)", ["<aucune>"] + cols, index=(idx_country+1 if idx_country>=0 else 0))

    st.subheader("3) Options")
    add_suffixes = st.checkbox("Ajouter automatiquement les suffixes Yahoo (.PA, .AS, .BR, .LS, .IR)", value=True)
    drop_dupes   = st.checkbox("Supprimer les doublons par ticker", value=True)

    # ---------- 4) Génération & Debug ----------
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

        # Nettoyage de base
        out["ticker"] = out["ticker"].replace({"nan":"", "None":""}).str.replace(r"\s+", "", regex=True)
        out = out[out["ticker"].str.len() > 0]

        # Stats debug AVANT suffixes / dédoublonnage
        n_total = len(out)
        n_nonnull = out["ticker"].notna().sum()
        n_unique = out["ticker"].nunique()
        st.info(f"Stats initiales — lignes: {n_total} | tickers non vides: {n_nonnull} | tickers uniques: {n_unique}")

        # Ajout suffixes Euronext selon exchange (meilleur-effort)
        def infer_suffix(ticker: str, exchange: str) -> str:
            if "." in ticker:
                return ticker
            e = (exchange or "").lower()
            if "paris" in e or "france" in e:         return f"{ticker}.PA"
            if "amsterdam" in e or "netherlands" in e: return f"{ticker}.AS"
            if "brussels" in e or "belgium" in e:     return f"{ticker}.BR"
            if "lisbon" in e or "portugal" in e:      return f"{ticker}.LS"
            if "dublin" in e or "ireland" in e:       return f"{ticker}.IR"
            return ticker

        if add_suffixes:
            out["ticker"] = [infer_suffix(t, ex) for t, ex in zip(out["ticker"], out["exchange"])]

        if drop_dupes:
            before = len(out)
            out = out.drop_duplicates(subset=["ticker"])
            st.write(f"Dédoublonnage: {before} → {len(out)} lignes.")

        # Colonnes finales
        out = out[["ticker","exchange","country_code","name"]]

        # Stats debug FINALES
        st.success(f"✅ {len(out)} lignes prêtes pour le screener.")
        st.dataframe(out.head(50), use_container_width=True)

        # Téléchargement
        csv_bytes = out.to_csv(index=False).encode("utf-8")
        st.download_button("💾 Télécharger pea_univers.csv (format screener)",
                           data=csv_bytes, file_name="pea_univers.csv", mime="text/csv")
