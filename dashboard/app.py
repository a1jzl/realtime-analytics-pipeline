"""Dashboard Streamlit affichant les indicateurs temps reel."""

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

DB_URL = "postgresql://analytics:analytics@localhost:5432/analytics"

st.set_page_config(page_title="Analytics temps reel", layout="wide")
st.title("Suivi du chiffre d'affaires en temps reel")


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


def load_data() -> pd.DataFrame:
    engine = get_engine()
    query = "SELECT * FROM revenue_by_window ORDER BY window_start DESC LIMIT 500"
    return pd.read_sql(query, engine)


data = load_data()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Chiffre d'affaires par fenetre de temps")
    revenue_over_time = data.groupby("window_start")["revenue"].sum().reset_index()
    st.line_chart(revenue_over_time, x="window_start", y="revenue")

with col2:
    st.subheader("Repartition par categorie")
    revenue_by_category = data.groupby("category")["revenue"].sum().reset_index()
    st.bar_chart(revenue_by_category, x="category", y="revenue")

st.subheader("Dernieres agregations recues")
st.dataframe(data.head(20))

st.button("Rafraichir")
