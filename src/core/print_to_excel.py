"""
Execute the SQL stored in src/logs/outputSQL.sql against local.duckdb
and save the result to output/query_result.xlsx
"""

import pathlib
import duckdb
import pandas as pd

# ── paths ────────────────────────────────────────────────────────────────────
ROOT          = pathlib.Path(r"C:\Users\akrsa\Documents\Abbott_AI_analysis_MVP")
SQL_FILE      = ROOT / "src" / "logs" / "outputSQL.sql"
DB_FILE       = ROOT / "local.duckdb"
OUT_DIR       = ROOT / "output"
OUT_FILE      = OUT_DIR / "query_result.xlsx"

# ensure output directory exists
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── load SQL ─────────────────────────────────────────────────────────────────
sql = SQL_FILE.read_text(encoding="utf-8")
# ── strip any Markdown code-block fences ──
clean_lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("```")]
sql = "\n".join(clean_lines).strip()
# ----------------------------------------

# ── run query ────────────────────────────────────────────────────────────────
con = duckdb.connect(str(DB_FILE))
df = con.execute(sql).fetch_df()
con.close()

# ── export to Excel ──────────────────────────────────────────────────────────
df.to_excel(OUT_FILE, index=False)
print(f"✅  Query ran successfully. Results written to: {OUT_FILE}")
