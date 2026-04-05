import pandas as pd
import requests
from dagster import asset
from dagster_snowflake import SnowflakeResource

# 1. API se Data Lena (Extraction)
@asset
def crypto_raw_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum,cardano,solana,ripple",
        "order": "market_cap_desc"
    }
    response = requests.get(url, params=params).json()
    
    # Snowflake table ke columns se match karne ke liye cleaning
    rows = []
    for coin in response:
        rows.append({
            "SYMBOL": coin["symbol"].upper(),
            "CURRENT_PRICE": coin["current_price"],
            "MARKET_CAP": coin["market_cap"]
        })
    return pd.DataFrame(rows)

# 2. Snowflake mein Data Load karna (Ab DuckDB ki zaroorat nahi)
@asset
def snowflake_load(crypto_raw_data, snowflake: SnowflakeResource):
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        
        # Purana data saaf karo (TRUNCATE)
        cursor.execute("TRUNCATE TABLE CRYPTO_DB.MAIN.RAW_PRICES")
        
        # Naya data insert karo
        for _, row in crypto_raw_data.iterrows():
            sql = f"""
                INSERT INTO CRYPTO_DB.MAIN.RAW_PRICES (SYMBOL, CURRENT_PRICE, MARKET_CAP)
                VALUES ('{row['SYMBOL']}', {row['CURRENT_PRICE']}, {row['MARKET_CAP']})
            """
            cursor.execute(sql)