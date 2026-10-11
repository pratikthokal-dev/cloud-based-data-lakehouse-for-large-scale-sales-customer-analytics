import pandas as pd
from pathlib import Path

PARQUET_DIR = Path("data/raw/parquet")

for file in PARQUET_DIR.glob("*.parquet"):

    print("\n" + "=" * 60)
    print(f"FILE: {file.name}")
    print("=" * 60)

    df = pd.read_parquet(file)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nSchema:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nSample:")
    print(df.head(3))