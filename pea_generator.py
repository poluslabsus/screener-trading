import streamlit as st
import pandas as pd
import io
import requests

st.set_page_config(page_title="PEA Universe Generator", layout="wide")
st.title("🗂 Générateur de liste PEA (Euronext)")

st.write(
    "Cet outil télécharge la liste publique d’actions européennes (Euronext) "
    "potentiellement éligibles au PEA, puis te permet de la sauvegarder en CSV."
)

url = "https://connect.euronext.com/media/169/download"

if st.button("📥 Télécharger la liste depuis Euronext"):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content))
        st.success(f"Fichier téléchargé : {len(df_raw)} lignes trouvées.")
        st.dataframe(df_raw.head(50), use_container_width=True)

        csv_bytes = df_raw.to_csv(index=False).encode("utf-8")
        st.download_button(
            "💾 Télécharger pea_univers.csv",
            data=csv_bytes,
            file_name="pea_univers.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Erreur : {e}")
        st.info("Le format du fichier Euronext peut changer, réessaie plus tard.")
