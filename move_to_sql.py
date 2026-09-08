import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# SETTINGS
# ============================================================

CSV_FILE = "stock_snapshot.csv"

DB_NAME = "stockdash" #should be renamed depending on your postgres
DB_USER = "postgres"
DB_PASSWORD = "*******" # Password removed for public git
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = "stock_data"

# ============================================================
# READ CSV
# ============================================================

df = pd.read_csv(CSV_FILE)

print(f"Loaded {len(df):,} rows")
print(f"Columns: {len(df.columns)}")

# ============================================================
# FIX DATE COLUMNS
# ============================================================

date_columns = [
    col for col in df.columns
    if col.startswith("Date_")
    or col in ["CurrentDate", "ExDividendDate", "NextEarningsDate"]
]

for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

# ============================================================
# CONNECT TO POSTGRESQL
# ============================================================

connection_string = (
    f"postgresql+psycopg2://"
    f"{DB_USER}:{DB_PASSWORD}@"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(connection_string)

# ============================================================
# UPLOAD
# ============================================================

df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",
    index=False,
    method="multi",
    chunksize=1000
)

print()
print("========================================")
print("UPLOAD COMPLETE")
print("========================================")
print(f"Table: {TABLE_NAME}")
print(f"Rows:  {len(df):,}")
print(f"Columns: {len(df.columns)}")