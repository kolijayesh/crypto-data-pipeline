import duckdb
import pandas as pd

# Database se connect karo
con = duckdb.connect("crypto_warehouse.duckdb")

# Analytics table ka data uthao
df = con.execute("SELECT * FROM crypto_analytics LIMIT 10").df()

# Print result
print("\n--- ASALI DATA ANALYTICS ---")
print(df)

con.close()