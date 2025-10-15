# --- Screener Contraction (Bollinger Squeeze + Volatilité basse) ---
# US + Europe (filtre PEA optionnel)
# Collez ce fichier dans: screener_pea_streamlit.py

from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# --------------------
# CONFIG de base
# --------------------
st.set_page_config(page_title="Screener Contraction (Bollinger Squeeze)", layout="wide")

EU_EEA_ISO2 = {
    # UE
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
    "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
    # EEE (hors UE)
    "NO","IS","LI",
}
EXCLUDE_ISO2 = {"GB","CH","US","CA"}  # utile si on veut forcer EU/EEE seulement

# --------------------
# UI
# --------------------
st.title("🔎 Screener de Contraction — Bollinger Squeeze + Volatilité basse")
st.caption("Analyse multi-marchés (US + Europe). Charge un univers de tickers, calcule le resserrement des Bandes de Bollinger + volatilité, et filtre les titres en contraction.")

with st.sidebar:
    st.header("⚙️ Paramètres")
    uploaded_csv = st.file_uploader("Univers de titres (CSV)", type=["csv"])
    st.markdown(
        "Le CSV doit contenir au moins **ticker** et idéalement **country_code** (ISO-2), **name**, **exchange**, **sector**.\n\n"
        "Exemples: `AAPL` (US), `MC.PA` (FR/Euronext), `ASML.AS` (NL), `SAP.DE` (DE), `ENEL.MI` (IT)"
    )

    # Fenêtres et seuils techniques
    lookback_days = st.number_input("Période d'historique (jours calendaires)", 120, 800, 260, step=10)
    bb_window = st.number_input("Période Bollinger (jours)", 10, 100, 20, step=1)
    bb_std = st.number_input("Écart-type Bollinger", 1.0, 3.5, 2.0, step=0.1)
    atr_window = st.number_input("Période ATR (jours)", 5, 50, 14, step=1)
    vol_window = st.number_input("Période volatilité (écart-type des retours, jours)", 5, 60, 20, step=1)

    st.divider()
    st.subheader("🎯 Critères de contraction")
    bbw_pct_threshold = st.slider(
        "Seuil de percentile du **Bollinger Band Width** (BBW) vs son historique (plus petit = plus serré)",
        1, 50, 20, help="Ex: 20 = BBW actuel est dans les 20% les plus bas de la période"
    )
    max_atr_pct = st.slider("ATR% max (ATR / Close, en %)", 0.1, 10.0, 2.0, step=0.1)
    max_sigma_pct = st.slider("Volatilité 20j max (σ retours * 100, en %)", 0.1, 10.0, 2.5, step=0.1)

    st.divider()
    st.subheader("🧹 Filtres pratiques")
    min_price = st.number_input("Prix minimum", 0.0, 10000.0, 1.0, step=0.5)
    min_avg_vol = st.number_input("Volume moyen 20j minimum", 0.0, 5_000_000.0, 50_000.0, step=5_000.0)
    force_pea = st.checkbox("Forcer pays UE/EEE (filtre PEA pays)", value=False)
    exclude_non_eee = st.checkbox("Exclure GB/CH/US/CA si pas de PEA forcé", value=False)

    st.divider()
    dl_cols = st.text_input(
        "Colonnes à exporter (séparées par des virgules)",
        value="ticker,name,exchange,country_code,close,avg_vol_20,bbw,bbw_percentile,atr_pct,sigma20_pct,contraction_score"
    )

# --------------------
# Helpers
# --------------------
def parse_universe(df: pd.DataFrame) -> pd.DataFrame:
    cols_map = {
        "ticker": "ticker", "symbol": "ticker",
        "exchange": "exchange", "mic": "mic",
        "name": "name", "company": "name",
        "country": "country_code", "country_code": "country_code", "country_iso2": "country_code",
        "sector": "sector",
    }
    out = {}
    for col in df.columns:
        key = col.strip().lower()
        if key in cols_map:
            out[cols_map[key]] = df[col].astype(str).str.strip()
    res = pd.DataFrame(out)
    if "ticker" not in res.columns:
        raise ValueError("Le CSV doit contenir au moins une colonne 'ticker' (ou 'symbol').")
    res["exchange"] = res.get("exchange", pd.Series([""]*len(res)))
    res["country_code"] = res.get("country_code", pd.Series([""]*len(res))).str.upper()
    res["name"] = res.get("name", pd.Series([""]*len(res)))
    res["sector"] = res.get("sector", pd.Series([""]*len(res)))
    return res.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

def is_pea_country(iso2: str) -> bool:
    return iso2 in EU_EEA_ISO2

def compute_indicators(px: pd.DataFrame, bb_win: int, bb_std: float, atr_win: int, vol_win: int):
    """px: DataFrame avec colonnes ['Open','High','Low','Close','Volume'] indexées en date"""
    df = px.copy()

    # Bollinger Bands
    sma = df["Close"].rolling(bb_win).mean()
    stdev = df["Close"].rolling(bb_win).std(ddof=0)
    upper = sma + bb_std * stdev
    lower = sma - bb_std * stdev
    bbw = (upper - lower) / sma  # largeur normalisée
    df["bbw"] = bbw

    # Percentile du BBW actuel vs historique (sur tout le lookback)
    # On calcule le rang percentile du dernier point par rapport à l'historique récent
    def last_percentile(series: pd.Series):
        s = series.dropna()
        if s.empty:
            return np.nan
        last = s.iloc[-1]
        rank = (s <= last).mean() * 100.0
        return rank

    # ATR (True Range)
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(atr_win).mean()
    atr_pct = (atr / close) * 100.0
    df["atr_pct"] = atr_pct

    # Volatilité (écart-type des rendements 1j)
    ret = close.pct_change()
    sigma20 = ret.rolling(vol_win).std(ddof=0) * 100.0
    df["sigma20_pct"] = sigma20

    # Valeurs du jour (dernière ligne)
    last = df.iloc[-1]
    out = {
        "close": float(last["Close"]),
        "avg_vol_20": float(df["Volume"].rolling(20).mean().iloc[-1]) if "Volume" in df else np.nan,
        "bbw": float(last["bbw"]) if not np.isnan(last["bbw"]) else np.nan,
        "bbw_percentile": float(last_percentile(df["bbw"])),
        "atr_pct": float(last["atr_pct"]) if not np.isnan(last["atr_pct"]) else np.nan,
        "sigma20_pct": float(last["sigma20_pct"]) if not np.isnan(last["sigma20_pct"]) else np.nan,
    }
    return out

def yf_bulk(tickers: list, start, end):
    data = yf.download(
        tickers=tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    out = {}
    if isinstance(data.columns, pd.MultiIndex):
        for t in tickers:
            try:
                df = data[t].dropna()
                if not df.empty: out[t] = df
            except Exception:
                pass
    else:
        out[tickers[0]] = data.dropna()
    return out

# --------------------
# MAIN
# --------------------
if uploaded_csv is None:
    st.info("➡️ Charge un CSV d’univers (tickers). Tu peux commencer petit (quelques dizaines) puis élargir.")
    st.stop()

try:
    uni_raw = pd.read_csv(uploaded_csv)
    universe = parse_universe(uni_raw)
except Exception as e:
    st.error(f"Erreur lecture CSV: {e}")
    st.stop()

# Filtres pays (PEA)
if force_pea:
    before = len(universe)
    universe = universe[universe["country_code"].apply(is_pea_country)]
    st.success(f"Filtre PEA (pays UE/EEE) appliqué : {before} → {len(universe)} titres.")
elif exclude_non_eee:
    before = len(universe)
    universe = universe[~universe["country_code"].isin(EXCLUDE_ISO2)]
    st.info(f"Exclusion GB/CH/US/CA : {before} → {len(universe)} titres.")

if universe.empty:
    st.warning("Univers vide après filtres pays. Vérifie les codes pays (ISO-2) ou désactive le filtre.")
    st.stop()

# Téléchargement des prix
end = datetime.utcnow()
start = end - timedelta(days=int(lookback_days))
tickers = universe["ticker"].tolist()

# Cap raisonnable pour la démo gratuite (évite timeouts)
CAP = st.number_input("Cap téléchargements (sécurité)", 10, 2000, min(500, len(tickers)), step=10)
tickers = tickers[:int(CAP)]

with st.spinner(f"Téléchargement des données pour {len(tickers)} tickers…"):
    prices_map = yf_bulk(tickers, start, end)

rows = []
for _, row in universe.iterrows():
    t = row["ticker"]
    px = prices_map.get(t)
    if px is None or px.empty or "Close" not in px.columns:
        continue

    try:
        metrics = compute_indicators(px, bb_window, bb_std, atr_window, vol_window)
    except Exception:
        continue

    # Filtres pratiques
    if not np.isnan(metrics["close"]) and metrics["close"] < min_price:
        continue
    if not np.isnan(metrics["avg_vol_20"]) and metrics["avg_vol_20"] < min_avg_vol:
        continue

    # Critères de contraction
    cond_bbw = (not np.isnan(metrics["bbw_percentile"])) and (metrics["bbw_percentile"] <= bbw_pct_threshold)
    cond_atr = (not np.isnan(metrics["atr_pct"])) and (metrics["atr_pct"] <= max_atr_pct)
    cond_sig = (not np.isnan(metrics["sigma20_pct"])) and (metrics["sigma20_pct"] <= max_sigma_pct)

    passes = cond_bbw and cond_atr and cond_sig

    # Score (plus bas = plus “serré” ; on l’inverse pour trier décroissant)
    # On fabrique un score où plus c’est serré, plus il est élevé (1.0 max ~ squeeze fort)
    score = 0.0
    if not np.isnan(metrics["bbw_percentile"]):
        score += (100 - metrics["bbw_percentile"]) / 100.0  # bas percentile -> haut score
    if not np.isnan(metrics["atr_pct"]):
        score += max(0.0, (max_atr_pct - metrics["atr_pct"]) / max_atr_pct) * 0.5
    if not np.isnan(metrics["sigma20_pct"]):
        score += max(0.0, (max_sigma_pct - metrics["sigma20_pct"]) / max_sigma_pct) * 0.5

    rows.append({
        "ticker": t,
        "name": row.get("name", ""),
        "exchange": row.get("exchange", ""),
        "country_code": row.get("country_code", ""),
        "sector": row.get("sector", ""),

        "close": metrics["close"],
        "avg_vol_20": metrics["avg_vol_20"],
        "bbw": metrics["bbw"],
        "bbw_percentile": metrics["bbw_percentile"],
        "atr_pct": metrics["atr_pct"],
        "sigma20_pct": metrics["sigma20_pct"],

        "contraction_match": passes,
        "contraction_score": round(score, 4),
    })

results = pd.DataFrame(rows)
if results.empty:
    st.warning("Aucun résultat (avec les seuils actuels). Essaie d’assouplir les sliders (BBW percentile plus haut, ATR% plus haut, etc.).")
    st.stop()

# Conserver uniquement les matchs
filtered = results[results["contraction_match"] == True].copy()
filtered = filtered.sort_values("contraction_score", ascending=False).reset_index(drop=True)

st.subheader("📈 Résultats — Contraction détectée")
st.caption("Triés par score de contraction (combinaison: BBW percentile bas + ATR% bas + σ20% basse).")
st.dataframe(filtered, use_container_width=True)

# Download CSV
cols = [c.strip() for c in dl_cols.split(",") if c.strip() in filtered.columns]
if not cols:
    cols = list(filtered.columns)
csv_bytes = filtered[cols].to_csv(index=False).encode("utf-8")
st.download_button("💾 Télécharger CSV", data=csv_bytes, file_name="screener_contraction_results.csv", mime="text/csv")

st.divider()
st.markdown("""
**Notes**
- *Contraction* ≈ **Bandes de Bollinger serrées** (BBW faible vs son propre historique) **ET** **volatilité basse** (ATR% et σ20% faibles).  
- Le **percentile BBW** compare la largeur actuelle à tout l’historique chargé (ex.: 20 = plus serrée que 80% des observations).  
- Les **tickers** doivent être fournis via un CSV d’univers (US + Europe). Pour PEA, fournis `country_code` (ISO-2) et active le filtre **“Forcer pays UE/EEE”**.
""")
# =============================================================
# SECTION FIXE : Génération du CSV d’univers PEA (Euronext)
# =============================================================
import io
import requests

st.header("🗂 Générer un univers PEA à jour")
st.write(
    "Clique sur le bouton ci-dessous pour télécharger la liste publique des actions européennes "
    "(Euronext, éligibles au PEA/PEA-PME)."
)

url_euronext = "https://connect.euronext.com/media/169/download"

if st.button("📥 Générer un CSV PEA depuis Euronext"):
    try:
        r = requests.get(url_euronext, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content))

        # Recherche des colonnes pertinentes (souvent variables)
        cols = {c.lower(): c for c in df_raw.columns}
        cand_name = [k for k in cols if "nom" in k or "name" in k or "issuer" in k or "company" in k]
        cand_ticker = [k for k in cols if "ticker" in k or "mnemo" in k or "symbol" in k]
        cand_market = [k for k in cols if "market" in k or "exchange" in k or "place" in k]
        cand_country = [k for k in cols if "pays" in k or "country" in k]

        name_col = cols.get(cand_name[0]) if cand_name else None
        ticker_col = cols.get(cand_ticker[0]) if cand_ticker else None
        market_col = cols.get(cand_market[0]) if cand_market else None
        country_col = cols.get(cand_country[0]) if cand_country else None

        work = pd.DataFrame()
        if ticker_col: work["ticker"] = df_raw[ticker_col].astype(str).str.strip()
        if name_col: work["name"] = df_raw[name_col].astype(str).str.strip()
        if market_col: work["exchange"] = df_raw[market_col].astype(str).str.strip()
        if country_col: work["country_code"] = df_raw[country_col].astype(str).str.upper()
        else: work["country_code"] = ""

        # Ajout suffixes Euronext (.PA, .AS, .BR, .LS)
        def add_suffix(row):
            t = row["ticker"]
            e = str(row.get("exchange","")).lower()
            if "." in t:
                return t
            if "paris" in e: return f"{t}.PA"
            if "amsterdam" in e: return f"{t}.AS"
            if "brussels" in e: return f"{t}.BR"
            if "lisbon" in e: return f"{t}.LS"
            return t
        work["ticker"] = work.apply(add_suffix, axis=1)

        pea_out = work.drop_duplicates(subset=["ticker"])
        st.success(f"✅ Univers PEA récupéré : {len(pea_out)} actions")
        st.dataframe(pea_out.head(50), use_container_width=True)

        csv_bytes = pea_out.to_csv(index=False).encode("utf-8")
        st.download_button(
            "💾 Télécharger pea_univers.csv",
            data=csv_bytes,
            file_name="pea_univers.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Erreur : {e}")
        st.info("Essaie de recharger la page ou d'attendre quelques minutes.")
