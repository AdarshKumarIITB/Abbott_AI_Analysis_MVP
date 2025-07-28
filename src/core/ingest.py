import yaml
import pandas as pd
import re
import duckdb
from pathlib import Path

def load_schema(path="registry/semantic_layer/analyzer.yaml"):
    with open(path) as f:
        meta = yaml.safe_load(f)
    # Build a dict: { column_name: { type, format, nullable, ... } }
    schema = { c["name"]: c for c in meta["columns"] }
    return schema

def read_and_filter(filepath, schema):
    """
    Reads CSV, XLSX, XLS or XLSB and retains only columns in schema.
    """
    ext = Path(filepath).suffix.lower()
    cols = list(schema.keys())

    if ext == ".csv":
        df = pd.read_csv(filepath, usecols=cols)
    elif ext in {".xlsx", ".xls", ".xlsb"}:
        # pandas will pick the right engine if you pass engine=...
        engine = {
            ".xlsx": "openpyxl",
            ".xls":  "xlrd",
            ".xlsb": "pyxlsb"
        }[ext]
        df = pd.read_excel(
            filepath,
            engine=engine,
            usecols=cols
        )
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    # Trim stray spaces in column names (just in case)
    df.rename(columns=str.strip, inplace=True)
    return df


def validate_and_cast(df, schema):
    errors = []
    for col, rules in schema.items():
        series = df[col]
        # 1) Nullability
        if not rules.get("nullable", True) and series.isna().any():
            errors.append(f"{col} has NULLs but is NOT NULL")
        # 2) Type‐casting
        t = rules["type"]
        if t == "integer":
            df[col] = series.astype("Int64")  # pandas nullable int
        elif t == "decimal":
            df[col] = series.astype(float)
        elif t == "date":
            df[col] = pd.to_datetime(series, format=rules["format"], errors="coerce")
        # 3) Regex / Range
        if "regex" in rules:
            bad = ~series.astype(str).str.match(rules["regex"])
            if bad.any():
                errors.append(f"{col} fails regex")
        if "allowable_range" in rules:
            mn = rules["allowable_range"]["min"]
            mx = rules["allowable_range"]["max"]
            bad = ~df[col].between(mn, mx)
            if bad.any():
                errors.append(f"{col} out of [{mn}, {mx}]")
    if errors:
        raise ValueError("Validation Errors:\n" + "\n".join(errors))
    return df

def ingest_to_duckdb(df, table_name="analyzer", db_path="local.duckdb"):
    con = duckdb.connect(db_path)
    # 1) Create table if not exists, based on df.dtypes
    # 2) Append all rows in one shot
    con.register("temp_df", df)
    con.execute(f"DROP TABLE IF EXISTS {table_name};")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df WHERE 0=1;")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df;")
    con.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python ingest.py <path/to/file.csv>")
        sys.exit(1)

    filepath = sys.argv[1]
    schema = load_schema()
    df = read_and_filter(filepath, schema)
    df = validate_and_cast(df, schema)
    ingest_to_duckdb(df)
    print("✅ Ingestion complete.")


