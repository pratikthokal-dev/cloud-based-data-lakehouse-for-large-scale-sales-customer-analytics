import pandas as pd
from pathlib import Path

SOURCE_DIR = Path("data/raw/online_retail_ii")
OUTPUT_DIR = Path("data/raw/parquet")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

xlsx_file = next(SOURCE_DIR.glob("*.xlsx"))

print(f"Reading: {xlsx_file}")

excel_file = pd.ExcelFile(xlsx_file)

for sheet in excel_file.sheet_names:

    print(f"\nProcessing sheet: {sheet}")

    df = pd.read_excel(
        xlsx_file,
        sheet_name=sheet
    )

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Enforce schema
    df["invoice"] = df["invoice"].astype("string")
    df["stockcode"] = df["stockcode"].astype("string")
    df["description"] = df["description"].astype("string")
    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    ).astype("Int64")
    df["invoicedate"] = pd.to_datetime(
        df["invoicedate"],
        errors="coerce"
    )
    df["price"] = pd.to_numeric(
        df["price"],
        errors="coerce"
    )
    df["customer_id"] = pd.to_numeric(
        df["customer_id"],
        errors="coerce"
    ).astype("Int64")
    df["country"] = df["country"].astype("string")

    # Create safe filename
    output_name = (
        sheet.lower()
        .replace(" ", "_")
        .replace("-", "_")
    )

    output_file = OUTPUT_DIR / f"{output_name}.parquet"

    df.to_parquet(
        output_file,
        index=False
    )

    print(f"Rows: {len(df):,}")
    print(f"Written: {output_file}")

print("\nRaw ingestion completed successfully.")