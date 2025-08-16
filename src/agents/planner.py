# src/agents/planner.py
"""
LangGraph Planner Agent - Improved Version
Enhanced ambiguity detection and clarification handling
"""

from typing import List, Dict, Optional
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from ..langchain_sql.schema_adapter import AbbottSchemaAdapter
import re


class WorkplanStep(BaseModel):
    id: str = Field(description="Unique identifier for this step (e.g., 'step_1')")
    type: str = Field(description="Type of operation: filter, aggregate, rank, compare, calculate")
    depends_on: List[str] = Field(default_factory=list, description="IDs of steps this depends on")
    question: str = Field(description="Natural-language description of what this step does")
    params: Dict[str, str] = Field(default_factory=dict, description="Parameters for this step")


class PlannerOutput(BaseModel):
    """
    Output from the planner.
    `ambiguities` is new in Phase 4; it stays empty unless we detect something
    that needs user clarification.
    """
    workplan: List[WorkplanStep]
    ambiguities: List[str] = Field(default_factory=list, description="List of clarifications needed")

    class Config:
        json_schema_extra = {
            "example": {
                "workplan": [
                    {
                        "id": "step_1",
                        "type": "filter",
                        "depends_on": [],
                        "question": "Get all data for DELHI zone in April",
                        "params": {"zone": "DELHI", "month": "Apr"}
                    },
                    {
                        "id": "step_2",
                        "type": "calculate",
                        "depends_on": ["step_1"],
                        "question": "Calculate primary value achievement percentage",
                        "params": {"metric": "primary_value_achievement_pct"}
                    }
                ],
                "ambiguities": [
                    "The word 'sales' is ambiguous – primary vs secondary & value vs units?"
                ]
            }
        }


class SimplePlanner:
    """
    Enhanced planner with improved ambiguity detection and context handling.
    """

    def __init__(self, schema_adapter: AbbottSchemaAdapter, model: str = "gpt-4o-mini"):
        self.schema_adapter = schema_adapter
        self.llm = ChatOpenAI(model=model, temperature=0)
        self.prompt = self._create_prompt()

    def _format_metrics_info(self) -> str:
        """Format available metrics from schema."""
        metrics = self.schema_adapter.schema.get('metrics', {})
        
        lines = ["Calculated Metrics:"]
        for metric_key, metric_info in metrics.items():
            name = metric_info.get('name', metric_key)
            desc = metric_info.get('description', 'No description')
            formula = metric_info.get('formula', '')
            lines.append(f"- {metric_key}: {name} - {desc}")
            if formula:
                lines.append(f"  Formula: {formula}")
        
        return "\n".join(lines)

    def _format_columns_info(self) -> str:
        """Format column information from schema."""
        columns = self.schema_adapter.schema.get('columns', [])
        
        lines = ["Database Columns:"]
        for col in columns:
            name = col.get('name')
            business_name = col.get('business_name', name)
            desc = col.get('business_description', '')
            examples = col.get('examples', col.get('example', []))
            
            lines.append(f"- {name} ({business_name}): {desc}")
            if examples:
                if isinstance(examples, list):
                    lines.append(f"  Examples: {', '.join(str(e) for e in examples)}")
                else:
                    lines.append(f"  Example: {examples}")
        
        return "\n".join(lines)

    def _format_business_rules(self) -> str:
        """Format business rules from schema."""
        business_context = self.schema_adapter.get_business_context()
        rules = business_context.get('rules', {})
        
        lines = ["Business Rules:"]
        
        # Data filters
        for filter_rule in rules.get('data_filters', []):
            lines.append(f"- {filter_rule.get('description', 'Unknown rule')}")
            if 'default_filter' in filter_rule:
                lines.append(f"  Default: {filter_rule['default_filter']}")
        
        # Calculation rules
        for calc_rule in rules.get('calculation_rules', []):
            lines.append(f"- {calc_rule.get('description', 'Unknown rule')}")
        
        # Vocabulary
        vocab = business_context.get('vocabulary', {})
        lines.append("\nBusiness Vocabulary:")
        for category, terms in vocab.items():
            lines.append(f"\n{category.replace('_', ' ').title()}:")
            for term, definition in terms.items():
                if isinstance(definition, dict):
                    if definition.get('ambiguous'):
                        lines.append(f"- '{term}' is ambiguous - needs clarification")
                    else:
                        lines.append(f"- '{term}' refers to specific metrics")
                elif isinstance(definition, str):
                    lines.append(f"- '{term}' means {definition}")
        
        return "\n".join(lines)

    def _create_prompt(self) -> ChatPromptTemplate:
        """Create the planning prompt with schema context."""
        metrics_info = self._format_metrics_info()
        columns_info = self._format_columns_info()
        business_rules = self._format_business_rules()
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a pharmaceutical sales analytics planner.

Your task is to break down complex queries into a sequence of simple, executable steps.

{columns_info}

{metrics_info}

{business_rules}

Step types you can use:
1. filter: Apply filters to data (e.g., zone='DELHI', month='Apr')
2. aggregate: Group data and calculate sums/averages
3. calculate: Calculate metrics like achievement %, growth %
4. rank: Sort and limit results (e.g., top 5, bottom 10)
5. compare: Compare different segments or time periods

Guidelines:
- Each step should do ONE thing
- Steps can depend on previous steps
- Always filter out Mth='All' unless specifically asked for totals
- For achievement/growth questions, break into: filter → calculate → analyze
- Use descriptive IDs like 'step_1', 'step_2', etc.
- Be specific with parameters - e.g., use exact zone names like 'DELHI'
- For queries about targets/achievement, you need to calculate achievement percentage

Example workplan for "Did Delhi zone achieve its targets in March?":
1. Filter data for Delhi zone and March
2. Calculate achievement percentage (Primary Value / Target Value)
3. Analyze if achievement >= 100%

Output a JSON with a 'workplan' array containing the steps."""
            ),
            ("human", "Query: {query}")
        ])
        
        return prompt.partial(
            columns_info=columns_info,
            metrics_info=metrics_info,
            business_rules=business_rules
        )

    def plan(self, query: str) -> PlannerOutput:
        """
        Return a workplan and list any places where we need clarification.
        """
        # First detect ambiguities
        ambiguities = self._detect_ambiguities(query)
        
        # Enhance query with any clarifications already provided
        enhanced_query = self._enhance_query_with_context(query)
        
        # Generate plan
        planner_chain = self.prompt | self.llm.with_structured_output(
            PlannerOutput,
            method="function_calling"
        )

        result: PlannerOutput = planner_chain.invoke({"query": enhanced_query})
        
        # Add detected ambiguities
        result.ambiguities = ambiguities
        
        return result

    def _detect_ambiguities(self, query: str) -> List[str]:
        """
        Detect ambiguities in the query that need clarification.
        """
        query_lc = query.lower()
        ambiguities: List[str] = []
        business_context = self.schema_adapter.get_business_context()
        vocab = business_context.get("vocabulary", {})

        # Check YAML-defined ambiguous terms
        for group in vocab.values():
            if isinstance(group, dict):
                for term, info in group.items():
                    if isinstance(info, dict) and info.get("ambiguous"):
                        # Check if term exists in query
                        if term in query_lc:
                            # Make sure it's not already clarified
                            clarified = False
                            if 'options' in info:
                                for option in info['options']:
                                    if option.lower() in query_lc:
                                        clarified = True
                                        break
                            
                            if not clarified:
                                clarification = info.get("clarification_needed", f"Please clarify '{term}'")
                                if clarification not in ambiguities:
                                    ambiguities.append(clarification)

        # Specific heuristics for common ambiguities
        
        # Achievement without specifying primary/secondary
        if ("achievement" in query_lc or "achieve" in query_lc or "target" in query_lc) and \
           not any(word in query_lc for word in ["primary", "secondary"]):
            if "Value Achievement (sales amount) or Unit Achievement (quantity sold)? Achievement to be calculated over Primary or Secondary ?" not in ambiguities:
                ambiguities.append("For achievement calculation: Primary or Secondary sales? Value or Units?")

        # Generic "sales" without qualifier
        if "sales" in query_lc and \
           not any(word in query_lc for word in ["primary", "secondary", "prim", "sec"]):
            if "'sales' – is that Primary or Secondary?  Value or Units?" not in ambiguities:
                ambiguities.append("When you say 'sales': Primary or Secondary? Value or Units?")

        # Time period ambiguities
        if any(phrase in query_lc for phrase in ["this month", "last month", "this quarter", "last quarter", "current"]) and \
           not any(month in query_lc for month in ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]) and \
           not any(q in query_lc for q in ["q1", "q2", "q3", "q4"]):
            ambiguities.append("Please specify the exact time period (e.g., 'Mar' for March, 'Q1' for first quarter)")

        return ambiguities

    def _enhance_query_with_context(self, query: str) -> str:
        """
        Enhance query with business context and defaults.
        """
        enhanced = query
        
        # Add context for common assumptions
        if "achievement" in query.lower() and "%" not in query:
            enhanced += " (Calculate achievement as percentage)"
        
        if "growth" in query.lower() and "yoy" not in query.lower() and "year" not in query.lower():
            enhanced += " (Calculate year-over-year growth)"
        
        return enhanced