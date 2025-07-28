#!/usr/bin/env python
"""
Run the SQL generator once and save the output.
Usage:  python scripts/run_sql_generator.py
"""

import json, pathlib, sys

# ---------- paths ----------
ROOT = pathlib.Path(__file__).resolve().parents[1]          # project root
sys.path.append(str(ROOT))                                  # make `src` import-able

INTENT_PATH  = ROOT / "src" / "utils" / "sampleJSON.json"
SCHEMA_PATH  = ROOT / "registry" / "semantic_layer" / "analyzer.yaml"
OUTPUT_DIR   = ROOT / "src" / "logs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH  = OUTPUT_DIR / "outputSQL.sql"

# ---------- import after path tweak ----------
from src.agents.SQLGenerator import SQLGenerator            # noqa: E402

# ---------- load intent ----------
with INTENT_PATH.open(encoding="utf-8") as f:
    intent = json.load(f)

# ---------- generate SQL ----------
generator = SQLGenerator(schema_path=SCHEMA_PATH)
sql = generator.generate(intent)

# ---------- persist ----------
OUTPUT_PATH.write_text(sql.strip() + "\n", encoding="utf-8")
print(f"✅  SQL written to {OUTPUT_PATH.relative_to(ROOT)}")
