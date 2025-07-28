# src/agents/prompts/sql_prompt.py
"""
SQL generation prompt for converting structured query intent to executable SQL.
"""

def get_sql_generation_prompt() -> str:
    return """
    You are an elite DuckDB SQL composer.

    SCHEMA
    ------
    {schema}

    QUERY INTENT (JSON)
    -------------------
    {intent}

    GOAL
    ----
    Write **one valid DuckDB query** that fulfils the intent exactly.

    CONSTRAINTS
    -----------
    1.  Use ONLY columns that appear in the schema.
    2.  Treat *dimensions* = intent["select"] ∪ intent["group_by"].
    3.  For every metric in intent["metrics"]:
        • Look up its **exact** formula in schema["metrics"][name]["formula"].  
        • Do **not** substitute different columns.
        • If the formula references raw columns, aggregate each column
            first in a CTE (e.g. `SUM(Sec_Value) AS Sec_Value_sum`).
    4.  Build the query in two tiers:

        -- Tier 1  (cte_base)  ------------------------------------------
        SELECT
            {{all dimensions}},
            {{one  SUM(col) AS col_sum  for every raw column that appears in ANY metric formula}}
        FROM {{intent.table}}
        WHERE {{all intent.where clauses}}
            {{IF intent.apply_standard_filters is true → AND Mth <> 'All'}}
        GROUP BY {{all dimensions}}

        -- Tier 2  (final SELECT)  ---------------------------------------
        SELECT
            {{all dimensions}},
            {{evaluate each metric formula using the *_sum aliases}} AS metric_alias
        FROM cte_base

        --  ⚠  IMPORTANT  ⚠
        --  Instead of HAVING, apply intent["having"] predicates in a WHERE
        --  clause on the already-computed metric aliases.  This avoids
        --  “column … must appear in the GROUP BY” errors.
        WHERE {{each condition in intent.having, referencing metric aliases}}

        ORDER BY {{intent.order_by}}
        LIMIT {{intent.limit or omit}}

    5.  The outer query **must not contain HAVING**; use the alias-based WHERE
        filter shown above.
    6.  ORDER BY must reference metric aliases, never raw expressions.
    7.  Do **NOT** wrap the SQL in markdown fences or prepend ```sql.
    8.  Return **only** plain SQL – no commentary.

    DOUBLE-CHECK
    ------------
    * Every column not in GROUP BY is aggregated.
    * Metric formulas match the YAML exactly.
    * Standard filter `Mth <> 'All'` is present when required.
    * The query executes in DuckDB without binder errors.

    ONLY return the final SQL.
    """.lstrip()
