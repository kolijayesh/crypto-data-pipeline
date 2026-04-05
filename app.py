import streamlit as st
import snowflake.connector
import pandas as pd

st.set_page_config(page_title="Crypto Analytics Dashboard", layout="wide")

st.title("🚀 Crypto Market Sentiment (Snowflake + dbt)")

# Snowflake Connection (Dhyan se details check karna)
def get_data():
    conn = snowflake.connector.connect(
        user='jayesh',
        password='@Jayesh19971997', # <--- Apna password dalo
        account='uoxcthr-vy59918',
        warehouse='CRYPTO_WH',
        database='CRYPTO_DB',
        schema='RAW'
    )
    query = "SELECT * FROM CRYPTO_ANALYTICS"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

try:
    data = get_data()

    # Metrics dikhane ke liye
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Assets", len(data))
    with col2:
        bullish_count = len(data[data['MARKET_SENTIMENT'] == 'Bullish'])
        st.metric("Bullish Assets", bullish_count)

    # Data Table
    st.subheader("Transformed Data from Snowflake")
    st.dataframe(data, use_container_width=True)

    # Chart
    st.subheader("Price Comparison")
    st.bar_chart(data.set_index('SYMBOL')['CURRENT_PRICE'])

except Exception as e:
    st.error(f"Error connecting to Snowflake: {e}")
