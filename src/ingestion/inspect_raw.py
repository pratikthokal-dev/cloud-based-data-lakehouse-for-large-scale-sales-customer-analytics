import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw/online_retail_ii")

xlsx_files = list(DATA_DIR.glob("*.xlsx"))

if not xlsx_files:
    raise FileNotFoundError(f"No Excel file found in {DATA_DIR}")

file_path = xlsx_files[0]

print("=" * 70)
print("DATASET INSPECTION")
print("=" * 70)

print(f"\nFile: {file_path}")
print(f"Size: {file_path.stat().st_size / (1024 * 1024):.2f} MB")

# Discover workbook structure
excel_file = pd.ExcelFile(file_path)

print("\nSheets:")
for sheet in excel_file.sheet_names:
    print(f"  - {sheet}")

# Inspect each sheet
for sheet in excel_file.sheet_names:

    print("\n" + "=" * 70)
    print(f"SHEET: {sheet}")
    print("=" * 70)

    df = pd.read_excel(file_path, sheet_name=sheet)

    print(f"\nRows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\nColumns:")
    for column in df.columns:
        print(f"  - {column}")

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isna().sum())

    print("\nDuplicate rows:")
    print(f"  {df.duplicated().sum():,}")

    print("\nFirst 5 rows:")
    print(df.head())

print("\n" + "=" * 70)
print("INSPECTION COMPLETE")
print("=" * 70)