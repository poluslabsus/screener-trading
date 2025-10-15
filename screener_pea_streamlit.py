
import io
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import yfinance as yf

# ---------------
# CONFIG
# ---------------
EU_EEA_ISO2 = {
    # EU
    "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
    "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
    # EEA (non-EU)
    "NO","IS","LI",
}
# Not eligible (examples): "GB","CH","US","CA"
EXCLUDE_ISO2 = {"GB","CH","US","CA"}

DEFAULT_LOOKBACK_DAYS = 260  # ~1y trading days
PRICE_INTERVAL = "1d"

st.set_page_config(page_title="PEA Screener (EU/EEA)", layout="wide")

st.title("🔎 PEA Screener — Actions éligibles (EU/EEE)")
st.caption("Charge un univers de tickers européens, filtre l’éligibilité PEA par pays (siège social EU/EEE), puis applique des filtres techniques/volume.")

# ---------------
# SIDEBAR
# ---------------
st.sidebar.header("⚙️ Paramètres")
uploaded_csv = st.sidebar.file_uploader("Univers de titres (CSV)", type=["csv"])

min_price = st.sidebar.number_input("Prix min (€)", value=1.0, step=0.5)
min_avg_vol = st.sidebar.number_input("Volume moyen min (sur 20j)", value=20000.0, step=1000.0)
min_mcap = st.sidebar.number_input("Capitalisation boursière min (€)", value=0.0, step=1_000_000.0, help="Laisse 0 si non disponible via Yahoo pour certains marchés.")
rsi_bounds = st.sidebar.slider("RSI(14) entre", 10, 90, (30, 70))
mom_window = st.sidebar.selectbox("Fenêtre Momentum (jours)", [20, 60, 125, 200], index=2)
sma_short = st.sidebar.number_input("SMA courte (jours)", value=20, step=1)
sma_long  = st.sidebar.number_input("SMA longue (jours)", value=50, step=1)

enable_pea_filter = st.sidebar.checkbox("Forcer filtre PEA (pays EU/EEE)", value=True)
exclude_st = st.sidebar.checkbox("Exclure tickers hors EU/EEE connus", value=True)

st.sidebar.divider()
download_cols = st.sidebar.text_input("Colonnes à exporter (séparées par des virgules)", value="ticker,name,exchange,country_code,close,avg_vol_20,rsi_14,mom,sma_short,sma_long,above_sma_long,mkt_cap,pe_ratio,sector")

# ---------------
# Helpers
# ---------------
def parse_universe(df: pd.DataFrame) -> pd.DataFrame:
    cols_map = {
        "ticker": "ticker",
        "symbol": "ticker",
        "isin": "isin",
        "exchange": "exchange",
        "mic": "mic",
        "name": "name",
        "company": "name",
        "country": "country_code",
        "country_code": "country_code",
        "country_iso2": "country_code",
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

def technicals_from_prices(prices: pd.DataFrame) -> pd.DataFrame:
    # Expect 'Close' and 'Volume'
    df = prices.copy()
    df["rsi_14"] = rsi(df["Close"], 14)
    df["sma_short"] = df["Close"].rolling(sma_short).mean()
    df["sma_long"]  = df["Close"].rolling(sma_long).mean()
    df["avg_vol_20"] = df["Volume"].rolling(20).mean()
    df[f"mom"] = df["Close"].pct_change(mom_window)
    return df

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=close.index).ewm(alpha=1/period, adjust=False).mean()
    roll_down = pd.Series(down, index=close.index).ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-9)
    return 100 - (100 / (1 + rs))

def fetch_yf_bulk(tickers: list, start: datetime, end: datetime, interval="1d"):
    # yfinance multi-download for speed; returns dict of DataFrames per ticker
    data = yf.download(
        tickers=tickers,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )
    out = {}
    # yfinance returns multi-index columns when multiple tickers
    if isinstance(data.columns, pd.MultiIndex):
        for t in tickers:
            try:
                df = data[t].dropna()
                if not df.empty:
                    out[t] = df
            except Exception:
                pass
    else:
        # Single ticker case
        out[tickers[0]] = data.dropna()
    return out

def fetch_static_fundamentals(ticker: str):
    # Use yfinance info (best-effort; can be slow/incomplete by market)
    try:
        info = yf.Ticker(ticker).fast_info
        mkt_cap = info.get("market_cap", np.nan)
        pe = np.nan  # fast_info has no PE; fallback to .info (slow) only when needed
    except Exception:
        mkt_cap = np.nan
        pe = np.nan
    return mkt_cap, pe

# ---------------
# MAIN
# ---------------
if uploaded_csv is None:
    st.info("Charge un CSV d’univers pour commencer. Colonnes acceptées: ticker, exchange, country_code, name, sector.")
    st.stop()

try:
    uni_raw = pd.read_csv(uploaded_csv)
    universe = parse_universe(uni_raw)
except Exception as e:
    st.error(f"Erreur lecture CSV: {e}")
    st.stop()

# PEA filter by country
if enable_pea_filter:
    before = len(universe)
    universe = universe[universe["country_code"].apply(is_pea_country)]
    st.success(f"Filtre PEA (pays EU/EEE) appliqué: {before} → {len(universe)} titres.")
elif exclude_st:
    before = len(universe)
    universe = universe[~universe["country_code"].isin(EXCLUDE_ISO2)]
    st.success(f"Exclusion pays non-UE/EEE courants (GB/CH/US/CA): {before} → {len(universe)} titres.")

if universe.empty:
    st.warning("Univers vide après filtres pays. Vérifie les codes pays ou désactive le filtre PEA.")
    st.stop()

# Fetch prices
end = datetime.utcnow()
start = end - timedelta(days=DEFAULT_LOOKBACK_DAYS)

tickers = universe["ticker"].tolist()[:400]  # safety cap for demo; increase as needed
with st.spinner(f"Téléchargement des prix pour {len(tickers)} tickers…"):
    prices_map = fetch_yf_bulk(tickers, start, end, interval=PRICE_INTERVAL)

rows = []
for _, row in universe.iterrows():
    t = row["ticker"]
    px = prices_map.get(t)
    if px is None or px.empty or "Close" not in px.columns:
        continue
    tech = technicals_from_prices(px)
    last = tech.iloc[-1]
    close = float(last["Close"])
    avg_vol_20 = float(last.get("avg_vol_20", np.nan))
    rsi_14 = float(last.get("rsi_14", np.nan))
    sma_s = float(last.get("sma_short", np.nan))
    sma_l = float(last.get("sma_long", np.nan))
    above_sma_long = bool(close > sma_l) if not np.isnan(sma_l) else False
    mom = float(last.get("mom", np.nan))

    # fundamentals (best-effort)
    mkt_cap, pe_ratio = fetch_static_fundamentals(t)

    rows.append({
        "ticker": t,
        "name": row.get("name", ""),
        "exchange": row.get("exchange", ""),
        "country_code": row.get("country_code", ""),
        "sector": row.get("sector", ""),
        "close": close,
        "avg_vol_20": avg_vol_20,
        "rsi_14": rsi_14,
        "mom": mom,
        "sma_short": sma_s,
        "sma_long": sma_l,
        "above_sma_long": above_sma_long,
        "mkt_cap": mkt_cap,
        "pe_ratio": pe_ratio,
    })

results = pd.DataFrame(rows)

# Filters
mask = (
    (results["close"] >= min_price) &
    (results["avg_vol_20"] >= min_avg_vol) &
    (results["rsi_14"].between(rsi_bounds[0], rsi_bounds[1], inclusive="both")) &
    (results["mkt_cap"] >= min_mcap)
)
filtered = results[mask].copy()

# Ranking example: combine momentum + trend
filtered["score"] = (
    filtered["mom"].fillna(0)*2 +
    filtered["above_sma_long"].astype(int)*0.5 +
    (filtered["rsi_14"].sub(50).abs().rsub(50)/50).fillna(0)*0.5
)

filtered = filtered.sort_values("score", ascending=False)

st.subheader("📈 Résultats filtrés")
st.dataframe(filtered.reset_index(drop=True), use_container_width=True)

# Download
cols = [c.strip() for c in download_cols.split(",") if c.strip() in filtered.columns]
if not cols:
    cols = list(filtered.columns)
csv = filtered[cols].to_csv(index=False).encode("utf-8")
st.download_button("💾 Télécharger CSV filtré", data=csv, file_name="pea_screener_results.csv", mime="text/csv")

st.divider()
st.caption("""
ℹ️ **Notes importantes**  
- L’éligibilité PEA réelle dépend du **siège social dans l’UE/EEE** et de l’assujettissement à l’IS local. Ici, on filtre par **code pays** fourni dans le CSV.  
- Les actions US **ne sont pas éligibles au PEA**. Pour exposer le marché US via PEA, utilise des **ETF éligibles PEA** (avec quota UE ≥ 75%) — à gérer dans un univers séparé.  
- Yahoo Finance peut manquer de données (volumes, capi) pour certains marchés; pour un usage pro, préfère une API marchande (EODHD, Polygon, Finnhub, Quandl/Nasdaq, etc.).
""")
