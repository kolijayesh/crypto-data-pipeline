import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# Page Setup
st.set_page_config(page_title="Crypto Analytics Dashboard", layout="wide")

st.title("🪙 Real-time Crypto Analytics")
st.markdown("Pipeline: **Dagster** ➡️ **dbt** ➡️ **DuckDB** ➡️ **Streamlit**")

# Load Data from DuckDB
def load_data():
    con = duckdb.connect("crypto_warehouse.duckdb")
    df = con.execute("SELECT * FROM crypto_analytics").df()
    con.close()
    return df

try:
    df = load_data()

    # --- Metrics Section ---
    col1, col2, col3 = st.columns(3)
    with col1:
        btc_price = df[df['symbol'] == 'btc']['current_price'].values[0]
        st.metric("Bitcoin Price", f"${btc_price:,.2f}")
    with col2:
        st.metric("Assets Tracked", len(df))
    with col3:
        st.metric("Database Status", "Live ✅")

    st.markdown("---")

    # --- Charts Section ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("💰 Current Prices")
        fig_bar = px.bar(df, x='symbol', y='current_price', color='symbol')
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with c2:
        st.subheader("📊 Market Sentiment")
        fig_pie = px.pie(df, names='market_sentiment', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

    # --- Raw Data Table ---
    st.subheader("📋 Detailed Data View")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Error: {e}")
    st.warning("Bhai, Dagster mein 'Materialize All' kiya kya? DuckDB file nahi mil rahi!")

if st.button('🔄 Refresh Dashboard'):
    st.rerun()