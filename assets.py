import pandas as pd  #for ETL
import requests #For fetching data via API
import duckdb #for store Data in DB 
from dagster import asset #for monitor data 

# 1. API se Data Lena (Extraction)
@asset
def crypto_raw_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    # Sirf kaam ke columns rakhna
    return df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume']]

# 2. Data ko DuckDB mein Save aur Clean karna (Transformation/Storage)
@asset(deps=[crypto_raw_data])
def clean_crypto_data(crypto_raw_data):
    con = duckdb.connect("crypto_warehouse.duckdb")
    
    # Raw table banana
    con.execute("CREATE OR REPLACE TABLE raw_prices AS SELECT * FROM crypto_raw_data")
    
    # Thodi analytics: High volume coins filter karna
    df_clean = con.execute("""
        SELECT *, 
        CASE WHEN total_volume > 1000000000 THEN 'High' ELSE 'Low' END as volume_category
        FROM raw_prices
    """).df()
    
    con.close()
    return df_clean