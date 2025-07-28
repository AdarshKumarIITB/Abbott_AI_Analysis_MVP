# src/agents/clarifier.py
"""
Minimal Query Clarification Agent using schema lookups.
"""

import json
import yaml
from typing import Dict, List, Optional, Any
from pathlib import Path
import openai
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv

load_dotenv()

from .prompts.clarifier_prompt import (
    get_initial_clarifier_prompt,
    get_detailed_clarifier_prompt,
    get_clarification_refinement_prompt  # <-- added import for refinement prompt
)

from utils.audit import new_run_id, write_audit


class TimeContext(BaseModel):
    """Time-related aspects of the query"""
    primary_period: Optional[str] = None
    comparison_period: Optional[str] = None
    year_over_year: bool = False


class Ambiguity(BaseModel):
    """Ambiguity that needs clarification"""
    issue: str
    suggested_clarification: str


class QueryIntent(BaseModel):
    """Final structured query intent"""
    understood_query: str
    metrics_needed: List[str] = []
    calculated_metrics: List[str] = []
    dimension_hierarchies: List[str] = []
    granularity: Dict[str, List[str]] = {}              
    time_context: TimeContext = Field(default_factory=TimeContext)
    filters: Dict[str, Any] = {}
    ambiguities: List[Ambiguity] = []
    requires_clarification: bool = False


class SchemaLookup:
    """Handles schema lookups from analyzer.yaml"""
    
    def __init__(self, schema_path: str = "registry/semantic_layer/analyzer.yaml"):
        with open(schema_path, 'r') as f:
            self.schema = yaml.safe_load(f)
        self._cache = {}
    
    def get_sections(self, sections: List[str]) -> dict:
        """Get specific sections from schema"""
        result = {}
        for section in sections:
            if section == "columns":
                # Return simplified column info
                result["columns"] = [
                    {
                        "name": col["name"],
                        "type": col.get("type"),
                        "business_name": col.get("business_name", col["name"]),
                        "description": col.get("business_description", ""),
                        "aliases": col.get("aliases", [])
                    }
                    for col in self.schema.get("columns", [])
                ]
            elif section in self.schema:
                result[section] = self.schema[section]
        return result
    
    def lookup_term(self, term: str) -> Optional[Any]:
        """Look up a specific term in mappings"""
        term_lower = term.lower()
        return self.schema.get("term_mappings", {}).get(term_lower)
    
    def get_metric(self, metric_id: str) -> Optional[dict]:
        """Get a specific metric definition"""
        return self.schema.get("metrics", {}).get(metric_id)


class QueryClarifier:
    """Minimal clarifier using schema lookups.
    
    This class is responsible for converting a user's query into a structured query intent.
    It uses a two-phase process: first obtaining a basic initial understanding,
    and then refining that understanding with schema lookups.
    """

    def __init__(self, schema_path: str = "registry/semantic_layer/analyzer.yaml"):
        # Initialize the QueryClarifier with a path to the schema file (default is analyzer.yaml)
        self.schema_lookup = SchemaLookup(schema_path)   # Create a SchemaLookup instance using the given schema file
        self.original_query = ""                           # Initialize an empty string to store the original user query
        self.numeric_cols = {
            col["name"]
            for col in self.schema_lookup.get_sections(["columns"])["columns"]
            if str(col.get("type", "")).lower().startswith(("int", "float", "decimal"))
        }


    def _normalize_intent(self, data: dict) -> dict:
        # collapse None/"" → empty structures
        for k in ("metrics_needed", "granularity", "time_context"):
            if not data.get(k):
                data[k] = {} if k != "metrics_needed" else []
    
        # derive the flag from ambiguities
        data["requires_clarification"] = bool(data.get("ambiguities"))
    
        # guarantee every ambiguity has the right keys & no code
        clean = []
        for a in data.get("ambiguities", []):
            issue = str(a.get("issue", "")).strip()
            question = str(a.get("suggested_clarification", "")).strip()
            if ("`" in issue or "`" in question):
                issue = "invalid_content"
                question = "Please clarify your last request."
            clean.append({"issue": issue, "suggested_clarification": question})
        data["ambiguities"] = clean
        return data

    def clarify(self, user_query: str) -> QueryIntent:
        """Two-phase clarification with schema lookup.
        
        Phase 1: Generate an initial understanding without schema influence.
        Phase 2: If needed, enrich the response with schema details to produce a full query intent.
        """
        self.run_id = new_run_id()           # ← NEW
        self.audit  = True

        self.original_query = user_query  # Save the original query to an instance variable

        # Phase 1: Prepare initial prompt using a helper function and format it with the user query.
        initial_prompt = get_initial_clarifier_prompt().format(user_query=user_query)
        if self.audit:
            write_audit(self.run_id, "prompt_phase1", initial_prompt)
        # Call the language model to get an initial response based on the prompt.
        initial_response = self._call_llm(initial_prompt)

        if self.audit:
            write_audit(self.run_id, "raw_response_phase1", initial_response)

        # Parse the JSON response from the language model into a Python dictionary.
        initial_understanding = json.loads(initial_response)

        # Phase 2: Check if schema lookup is needed based on the initial understanding.
        if initial_understanding.get("needs_schema_lookup", True):
            # Retrieve the list of schema sections needed (defaulting to metrics, term_mappings, and columns).
            sections_needed = initial_understanding.get("schema_sections_needed", ["metrics", "term_mappings", "columns"])
            # Look up these sections in the schema using the SchemaLookup instance.
            schema_context = self.schema_lookup.get_sections(sections_needed)

            # Prepare a more detailed prompt using another helper function. It includes:
            # - the original user query,
            # - the initial understanding dumped as JSON,
            # - and the retrieved schema context dumped as pretty JSON.
            detailed_prompt = get_detailed_clarifier_prompt().format(
                user_query=user_query,
                initial_understanding=json.dumps(initial_understanding),
                schema_context=json.dumps(schema_context, indent=2)
            )
            if self.audit:
                write_audit(self.run_id, "prompt_phase2", detailed_prompt)

            # Call the language model with the detailed prompt to get the final query intent.
            final_response = self._call_llm(detailed_prompt)

            if self.audit:
                write_audit(self.run_id, "raw_response_phase2", final_response)

            # Parse the final response from JSON into a dictionary.
            intent_data = json.loads(final_response)
            # ---- NEW: Normalize intent data right after parsing the final response.
            intent_data = self._normalize_intent(intent_data)
        else:
            # If no schema lookup is needed, convert the initial understanding into intent format directly.
            intent_data = self._convert_initial_to_intent(initial_understanding)
        
        # ---- sanity check: if any critical section is empty, force clarification
        critical_keys = ["metrics_needed", "granularity", "time_context"]
        if (not intent_data.get("ambiguities")) and any(
                not intent_data.get(k) for k in critical_keys):
            intent_data["ambiguities"] = [{
                "issue": "missing_fields",
                "suggested_clarification": "Some required fields were empty – please clarify."
            }]
            intent_data["requires_clarification"] = True

        # Create a QueryIntent object from the intent data using Pydantic model validation.
        intent = QueryIntent(**intent_data)
        # Apply any additional business rules to the intent before returning it.
        if self.audit:
            write_audit(self.run_id, "final_intent", intent.model_dump())
        return self._apply_business_rules(intent)

    def _call_llm(self, prompt: str) -> str:
        """Call LLM with the given prompt.
        
        This helper method interacts with the OpenAI API to obtain a response for a given prompt.
        """
            
        try:
            # Retrieve the OpenAI API key from the environment variables.
            api_key = os.getenv("OPENAI_API_KEY")
                
            # Instantiate the OpenAI client with the provided API key.
            client = openai.OpenAI(api_key=api_key)
            
            # Call the chat completion endpoint with our prompt.
            response = client.chat.completions.create(
                model="gpt-4-1106-preview",  # Specify the model (choosing a lower-cost one for MVP)
                messages=[
                    {"role": "system", "content": "You are a data analyst assistant. Respond ONLY in valid **JSON** matching the schema I give you."},  # System message setting context for the model.
                    {"role": "user", "content": prompt}  # User message containing the prompt.
                ],
                temperature=0.0,  # Use a low temperature for more deterministic responses.
                response_format={"type": "json_object"}  # Expect the response to be in JSON format.
            )
            
            # Return the content of the first choice if available, otherwise an empty string.
            return response.choices[0].message.content or ""
            
        except Exception as e:
            # In case of any exceptions (e.g., network issues or API errors), print the error.
            print(f"LLM Error: {e}")
            return ""

    def _apply_business_rules(self, intent: QueryIntent) -> QueryIntent:
        """Apply analyzer-specific rules.
        
        This method adjusts the query intent based on predefined business rules.
        """
        # Rule: If the calculated metrics include achievement_pct but metrics_needed does not include Prim_Value or Sec_Value,
        # then add Prim_Value and Tgt_Value to metrics_needed.
        if "achievement_pct" in intent.calculated_metrics and not any(m in intent.metrics_needed for m in ["Prim_Value", "Sec_Value"]):
            intent.metrics_needed.extend(["Prim_Value", "Tgt_Value"])
        
        # Rule: Always exclude the 'All' month. If there is no filter defined for "Mth", then set a filter to exclude "All".
        if not any(f.get("Mth") for f in [intent.filters]):
            intent.filters["Mth"] = {"$ne": "All"}
        
        self._dedupe_metrics(intent)
        # Return the modified intent.

        # Normalize month values in time context.
        tc = intent.time_context
        tc.primary_period = self._normalise_month(tc.primary_period or "")
        tc.comparison_period = self._normalise_month(tc.comparison_period or "")

        self._sync_hierarchies(intent)
        
        return intent
    
    def _sync_hierarchies(self, intent: QueryIntent) -> None:
        """If no dimension hierarchies are provided, use keys from granularity."""
        if not intent.dimension_hierarchies and intent.granularity:
            intent.dimension_hierarchies = list(intent.granularity.keys())
    
    def _dedupe_metrics(self, intent: QueryIntent) -> None:
        """
        1. Remove duplicates while preserving order.
        2. If the schema exposes datatypes, keep only numeric columns.
        (If no datatypes are present, we skip the numeric filter.)
        """
        # de-duplicate
        seen = set()
        intent.metrics_needed = [
            m for m in intent.metrics_needed if not (m in seen or seen.add(m))
        ]

        # numeric-only filter (optional)
        if self.numeric_cols:                       # <- empty means “no type info”
            intent.metrics_needed = [
                m for m in intent.metrics_needed if m in self.numeric_cols
            ]
    # ───────────────────────────────────────────────────────────────────


    def _normalise_month(self, month: str) -> str:
        """Normalize full month names to abbreviations."""
        mapping = {"april": "Apr", "march": "Mar", "january": "Jan", "february": "Feb",
                   "may": "May", "june": "Jun", "july": "Jul", "august": "Aug",
                   "september": "Sep", "october": "Oct", "november": "Nov", "december": "Dec"}
        return mapping.get(month.lower(), month)


    def _convert_initial_to_intent(self, initial: dict) -> dict:
        """Convert initial understanding to intent format.
        
        This method takes the initial LLM response and converts it into the minimal
        intent structure, marking that further clarification is required.
        """
        # Construct and return a dictionary with default values for query intent fields.
        return {
            "understood_query": f"Analyze {initial.get('query_type', 'data')}",  # Describe the query based on initial query_type
            "metrics_needed": [],      # No specific metrics provided initially
            "calculated_metrics": [],  # No already calculated metrics
            "dimension_hierarchies": [],   # e.g. "geography"
            "granularity": {},             # e.g. ["Terr_Code"]            
            "time_context": {},        # Empty time context
            "filters": {},             # No filter conditions specified
            "ambiguities": [],         # No ambiguities detected
            "requires_clarification": True  # Mark that the query requires further clarification
        }
    
    def _merge_intents(self, base: dict, patch: dict) -> dict:
        """
        Accepts a partial update (patch) from the LLM and overlays it
        onto the previous full intent (base). Empty / null values are ignored.
        """
        for k, v in patch.items():
            if v in (None, "", [], {}):
                # ignore blank fields coming from the model
                continue
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                base[k].update(v)
            else:
                base[k] = v
        return base

    def refine_with_clarification(self, user_reply: str, previous_intent: QueryIntent) -> QueryIntent:
        """
        Refine the query intent using follow-up clarification from the user.
        Constructs a refinement prompt using the original query, the clarification question,
        the user's follow-up response, and the previous intent data.
        Then merges the partial patch from the LLM into the previous full intent.
        """
        prompt = get_clarification_refinement_prompt().format(
            original_query=self.original_query,
            clarification_question=previous_intent.ambiguities[0].suggested_clarification,
            user_response=user_reply,
            previous_understanding=json.dumps(previous_intent.model_dump())
        )
        response_json = self._call_llm(prompt)
        patch_dict = json.loads(response_json)      # may be partial
        full_dict  = self._merge_intents(previous_intent.model_dump(), patch_dict)
        full_dict  = self._normalize_intent(full_dict)   # reuse your cleaner
        updated = QueryIntent(**full_dict)
        self._sync_hierarchies(updated)
        return updated