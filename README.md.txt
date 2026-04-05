# 🪙 Crypto Data Pipeline (Modern Data Stack)

An end-to-end data engineering pipeline that extracts crypto prices, transforms them using dbt, and visualizes them on a Streamlit dashboard.

## 🛠️ Tech Stack
- **Orchestration:** Dagster
- **Transformation:** dbt
- **Warehouse:** DuckDB
- **Visualization:** Streamlit
- **Version Control:** GitHub

## 🚀 How to Run
1. Clone the repo
2. Run `pip install -r requirements.txt`
3. Start Dagster: `dagster dev`
4. Run Dashboard: `streamlit run dashboard.py`