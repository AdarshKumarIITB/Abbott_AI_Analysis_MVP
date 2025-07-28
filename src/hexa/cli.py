# src/hexa/cli.py

from pathlib import Path                   # Imports Path for handling filesystem paths in an OS-independent way
import sys                                 # Imports sys to manipulate the Python runtime environment

import typer                               # Imports Typer for building CLI applications
from rich.console import Console           # Imports Console from rich for styled terminal output
from rich.text import Text                 # Imports Text from rich for colored/styled text

# -- make sure the sibling "core" package is importable -----------------
SRC_DIR = Path(__file__).resolve().parents[1]      # Gets the parent directory of the current file's parent (i.e., the 'src' directory)
sys.path.insert(0, str(SRC_DIR))                   # Adds 'src' directory to the Python path so 'core' can be imported

from core.ingest import (                         # Imports functions from the core.ingest module
    load_schema,                                  #   - load_schema: loads a schema definition
    read_and_filter,                              #   - read_and_filter: reads a file and filters data based on schema
    validate_and_cast,                            #   - validate_and_cast: validates and casts data types
    ingest_to_duckdb,                             #   - ingest_to_duckdb: loads data into DuckDB
)

# ----------------------------------------------------------------------
console = Console()
app = typer.Typer(help="Hexa ⬣  — data-loading CLI")

@app.callback()
def main():
    """
    Callback to enforce subcommand mode.
    """
    pass

@app.command("load")
def load(filename: str):
    """
    Load *filename* (found in the local `data/` folder) into DuckDB
    using the semantic-layer rules in `registry/semantic_layer/analyzer.yaml`.
    """
    data_dir   = Path.cwd() / "data"
    file_path  = data_dir / filename
    schema_path = Path.cwd() / "registry" / "semantic_layer" / "analyzer.yaml"

    # sanity checks -----------------------------------------------------
    if not data_dir.exists():
        console.print(Text(f"❌ 'data' directory not found: {data_dir}", style="red"))
        raise typer.Exit(1)

    if not file_path.exists():
        console.print(Text(f"❌ File not found: {file_path}", style="red"))
        raise typer.Exit(1)

    if not schema_path.exists():
        console.print(Text(f"❌ Schema file missing: {schema_path}", style="red"))
        raise typer.Exit(1)

    # ingestion pipeline -----------------------------------------------
    try:
        console.print(f"📊 Loading {filename} …")
        schema = load_schema(str(schema_path))
        df     = read_and_filter(str(file_path), schema)
        df     = validate_and_cast(df, schema)
        ingest_to_duckdb(df)
        console.print(Text(f"✅ Load successful – {len(df):,} rows ingested", style="green"))
    except Exception as exc:
        console.print(Text(f"❌ Load failed: {exc}", style="red"))
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
