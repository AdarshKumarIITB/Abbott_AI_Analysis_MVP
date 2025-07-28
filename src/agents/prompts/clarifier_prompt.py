# src/agents/prompts/clarifier_prompt.py
"""
Prompt templates for the two-phase QueryClarifier.
The agent first extracts rough intent (no schema),
then receives a trimmed schema context to produce a full QueryIntent.
"""

def get_initial_clarifier_prompt() -> str:
    """
    Phase-1: no schema yet - just identify coarse features that drive
    what parts of the schema we should send next. Initial prompt without schema details - just understand the query structure.
    """
    
    prompt = """You are analysing a natural-language question about pharmaceutical sales data. Extract the key components.

    USER QUERY: {user_query}

    Extract **only** high-level cues and respond in JSON:

    • "query_type" - one of [achievement_analysis, growth_analysis,comparison, ranking, descriptive].
    • "time_references" - calendar words you spot (e.g. "April", "YoY").
    • "entity_references" - text snippets that look like brands, zones, product names, head-quarters, etc.
    • "metric_references" - business metrics mentioned (achievment, growth, performance).
    • "needs_schema_lookup" - true unless the query is entirely unambiguous.
    • "schema_sections_needed" - list of schema blocks you expect are useful. Default to ["metrics","term_mappings","columns","hierarchies"].

    Respond in JSON. Example output: 
    {{
    "query_type": "achievement_analysis",
    "time_references": ["April","March"],
    "entity_references": ["North zone","focus brands", "top 5"],
    "metric_references": ["achievement","performance", "growth"],
    "needs_schema_lookup": true,
    "schema_sections_needed": ["metrics","term_mappings","columns","hierarchies", "term_mappings"]
    }}

    """
    return prompt


def get_detailed_clarifier_prompt() -> str:
    """
    Phase-2: full schema context available - produce the final structured
    QueryIntent with multiple hierarchies + per-hierarchy granularity.      
    """
    
    prompt = """You now have the user query, an initial interpretation, **and** the relevant sections of the semantic schema.

    USER QUERY: {user_query}

    INITIAL UNDERSTANDING: {initial_understanding}

    SCHEMA CONTEXT (trimmed JSON):
    {schema_context}
    # Each column =  {{name, type, business_name, aliases}}
    # Metrics are objects with {{formula, columns_used, description}}
    # Each hierarchy = {{name, levels[*].name & columns[*]}}

    Build the final intent object. **If you are not 100 '%' sure of ANY value, leave it null/empty and add an item to "ambiguities".  Never invent.**

    Guidelines:
    1. Only include columns whose schema **type is numeric** (int / float / decimal) in "metrics_needed".
    2. Non-numeric columns belong in "granularity" or "filters".
    3. Allow MULTIPLE hierarchies:
        "dimension_hierarchies": ["geography","product"]
    and for each, specify the *deepest* level(s) referenced by the user:
        "granularity": {{
            "geography": ["Zone","Terr_Code"],   // drill-down path
            "product":   ["Brand"]               // single level
        }}
    4. If the user clearly wants a full drill-down (top→bottom) put the exact drill down. If unsure about depth, add an ambiguity asking for clarity.
    
    Respond **only** in JSON.  Two examples, one complete and one that needs clarification, are shown so you understand when to ask:

    Example 1 (complete):
    {{
        "understood_query": "Clear explanation",
        "metrics_needed": ["Prim_Value","Tgt_Value"],
        "calculated_metrics": ["primary_achievement_pct", "yoy_growth_pct"],
        "dimension_hierarchies": ["geography","product"],
        "granularity": {{
            "geography": ["Terr_Code"],
            "product":   ["Brand"]
        }},
        "time_context": {{
            "primary_period": "Apr",
            "comparison_period": "Mar",
            "year_over_year": false
        }},
        "filters": {{
            "Mth": "Apr",
            "Status": "FOCUS"
        }},
        "ambiguities": [
            {{
                "issue": "description",
                "suggested_clarification": "question to ask"
            }}
        ],
        "requires_clarification": true/false
    }}

    Example 2 (needs clarification):
    {{
        "understood_query": "Unclear query",
        "metrics_needed": [],
        "calculated_metrics": [],
        "dimension_hierarchies": [],
        "granularity": {{}},
        "time_context": {{}},
        "filters": {{}},
        "ambiguities": [
            {{
                "issue": "Unclear about the depth of geography hierarchy",
                "suggested_clarification": "What level of geography do you want to see?"
            }}
        ],
        "requires_clarification": true
    }}
    """
    return prompt


# ──────────────────────────────────────────────────────────
def get_clarification_refinement_prompt() -> str:
    """
    Phase-3 prompt: user has answered the follow-up question.  Update the intent.
    All {{ }} are escaped so .format() sees only the three placeholders.
    """
    return """\
    You are refining a query intent for pharmaceutical sales data.

    ORIGINAL QUERY:
    {original_query}

    YOUR CLARIFICATION QUESTION:
    {clarification_question}

    USER'S ANSWER:
    {user_response}

    PREVIOUS INTENT (JSON):
    {previous_understanding}

    Update the intent JSON, resolving the ambiguity.  Respond **only** in JSON
    that matches the same schema: metrics_needed, calculated_metrics,
    dimension_hierarchies, granularity {{hierarchies: [levels]}}, time_context,
    filters, ambiguities, requires_clarification.
    """
