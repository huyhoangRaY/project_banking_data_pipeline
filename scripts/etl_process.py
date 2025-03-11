import pandas as pd
import psycopg2
import yaml

with open("config/db_config.yaml", "r") as file:
    config = yaml.safe_load(file)["postgres"]

conn = psycopg2.connect(
    host=config["host"],
    database=config["database"],
    user=config["user"],
    password=config["password"],
    port=config["port"]
)
cursor = conn.cursor()

df = pd.read_csv("data/data.csv")

print("📌 Column names before renaming:", df.columns.tolist())

df.rename(columns={
    "AccountNumber": "account_number",
    "CustomerName": "customer_name",
    "Balance": "balance",
    "Currency": "currency",
    "TransactionDate": "transaction_date"
}, inplace=True)

print("🔍 Column names after renaming:", df.columns.tolist())

df.dropna(inplace=True)

df["account_number"] = df["account_number"].astype(str)
df["customer_name"] = df["customer_name"].astype(str)
df["balance"] = df["balance"].astype(float)
df["currency"] = df["currency"].astype(str)
df["transaction_date"] = pd.to_datetime(df["transaction_date"], format="%Y-%m-%d")  # Chuyển đổi ngày tháng

df["customer_category"] = df["balance"].apply(lambda x: "VIP" if x > 50000 else "Regular")

create_table_query = """
CREATE TABLE IF NOT EXISTS banking_staging (
    account_number VARCHAR(20) PRIMARY KEY,
    customer_name TEXT,
    balance FLOAT,
    currency VARCHAR(10),
    transaction_date DATE,  -- Thêm cột transaction_date
    customer_category VARCHAR(10)
);
"""
cursor.execute(create_table_query)
conn.commit()

insert_query = """
INSERT INTO banking_staging (account_number, customer_name, balance, currency, transaction_date, customer_category)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (account_number) DO NOTHING;
"""

for row in df.itertuples(index=False, name=None):
    cursor.execute(insert_query, row)

conn.commit()
print("✅ ETL Process Completed Successfully!")

cursor.close()
conn.close()
