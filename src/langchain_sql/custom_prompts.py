from langchain_core.prompts import PromptTemplate
from typing import List, Dict, Any
import re

def escape_template_vars(text: str) -> str:
    """
    Escape single curly braces to prevent them from being interpreted as template variables.
    This is needed when including YAML content that has dictionary representations.
    """
    # Replace single { and } with double {{ and }} to escape them
    # But be careful not to escape already escaped ones or the template variables
    text = re.sub(r'(?<!\{)\{(?!\{)', '{{', text)
    text = re.sub(r'(?<!\})\}(?!\})', '}}', text)
    return text

def get_abbott_sql_prompt(examples: List[Dict[str, str]], schema_adapter: Any) -> PromptTemplate:
    """
    Create a custom prompt template for Abbott SQL generation.
    
    This function constructs a specialized prompt that:
    1. Includes Abbott-specific business rules and vocabulary
    2. Provides few-shot examples for better SQL generation
    3. Uses the correct format for LangChain SQL agents
    
    Args:
        examples: List of question-SQL pairs for few-shot learning
        schema_adapter: AbbottSchemaAdapter instance containing business context
    
    Returns:
        PromptTemplate configured for Abbott use case
    """
    
    # Extract business context and metrics from the schema
    business_context = schema_adapter.get_business_context()
    metrics = schema_adapter.get_metrics_definitions()
    hierarchies = schema_adapter.get_hierarchies()
    
    # Get table information - this includes column descriptions
    table_info = schema_adapter.get_custom_table_info()
    table_name = schema_adapter.get_table_name()
    table_description = table_info.get(table_name, "No table description available")
    
    # Escape the table description to avoid template variable conflicts
    table_description = escape_template_vars(table_description)
    
    # Format examples for inclusion in prompt
    formatted_examples = ""
    for i, example in enumerate(examples, 1):
        formatted_examples += f"""
Example {i}:
Question: {example['question']}
SQL: {example['query']}
"""
    
    # Format hierarchies
    hierarchy_info = ""
    for name, hierarchy in hierarchies.items():
        if 'relationships' in hierarchy:
            hierarchy_info += f"\n{hierarchy.get('name', name)} Hierarchy:\n"
            for rel in hierarchy['relationships']:
                hierarchy_info += f"  - {rel.get('parent', '')} → {rel.get('child', '')}\n"
    
    # Create the complete prompt template with required variables
    template = f"""You are an Abbott India sales analytics assistant with expertise in pharmaceutical sales data.

DATABASE SCHEMA AND CONTEXT:
{table_description}

HIERARCHIES:
{hierarchy_info}

CRITICAL RULES:
1. ALWAYS exclude Mth = 'All' unless specifically requested for totals
2. For "achievement", calculate as (Actual/Target) * 100
3. For "YoY growth", calculate as ((Current - LastYear) / LastYear) * 100
4. Treat NULL values as 0 in calculations using COALESCE
5. Use NULLIF(denominator, 0) to avoid division by zero
6. When grouping data, always include relevant dimension columns in SELECT

BUSINESS VOCABULARY:
- "sales" typically means Primary Value (Prim_Value) unless specified
- "secondary sales" means Sec_Value
- "achievement" means actual vs target percentage
- "focus brands" means Status = 'FOCUS'
- "underperforming" means achievement < 100%
- "territory" refers to Terr_Code
- "TBM" means Territory Business Manager

AVAILABLE CALCULATED METRICS:
{chr(10).join(f"- {k}: {v.get('formula', 'No formula')}" for k, v in metrics.items() if 'formula' in v)}

Here are some examples of questions and their corresponding SQL queries:
{formatted_examples}

You have access to tools to answer the question. Use them if needed.

Question: {{input}}
{{agent_scratchpad}}"""
    
    return PromptTemplate.from_template(template)

def get_validation_prompt() -> PromptTemplate:
    """
    Create a prompt for SQL validation.
    
    This prompt is used to check generated SQL for common mistakes
    before execution.
    
    Returns:
        PromptTemplate for SQL validation
    """
    return PromptTemplate.from_template("""
Review this SQL query for Abbott sales data analysis:

{sql_query}

Check for these common issues:
1. Is Mth = 'All' excluded (unless totals are requested)?
2. Are division by zero errors handled with NULLIF?
3. Are NULL values properly handled?
4. Is the business logic correct for the question asked?
5. Are column names correct according to the schema?

If there are issues, explain what's wrong. If the query is correct, confirm it's ready to execute.
""")