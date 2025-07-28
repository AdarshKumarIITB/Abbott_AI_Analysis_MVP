# src/agents/SQLGenerator.py
"""
SQL Generator Agent that converts structured query intent into executable SQL.
"""

import json
import yaml
import openai
import os
from typing import Dict
from dotenv import load_dotenv
import re
from textwrap import dedent

load_dotenv()

from .prompts.sql_prompt import get_sql_generation_prompt


class SQLGenerator:
    """Generates SQL from structured query intent."""
    
    def __init__(self, schema_path: str = "registry/semantic_layer/analyzer.yaml"):
        with open(schema_path, 'r') as f:
            self.schema = yaml.safe_load(f)
    
    def generate(self, intent: Dict) -> str:
        """
        Convert pre-validated JSON intent to DuckDB SQL.
        
        Args:
            intent: Dictionary following the structure in sampleJSON.json
            
        Returns:
            SQL query string
        """
        # Extract relevant schema sections
        schema_context = {
            "table": self.schema.get("table", "analyzer"),
            "columns": [
                {"name": col["name"], "type": col["type"]} 
                for col in self.schema.get("columns", [])
            ],
            "metrics": self.schema.get("metrics", {}),
            "hierarchies": self.schema.get("hierarchies", {})
        }
        
        # Get the prompt template and format it
        prompt = get_sql_generation_prompt().format(
            schema=json.dumps(schema_context, indent=2),
            intent=json.dumps(intent, indent=2)
        )
        
        # Call LLM once
        raw_sql = self._call_llm(prompt)
        
        return raw_sql.strip()
        
    
    def _call_llm(self, prompt: str) -> str:
        """Call LLM with the given prompt."""
        try:
            client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            response = client.chat.completions.create(
                model="gpt-4-1106-preview",
                messages=[
                    {"role": "system", "content": "You are a SQL expert. OUTPUT **PLAIN SQL ONLY** ""no markdown fences, no ```sql tag, no commentary. " "If you add anything except SQL, the answer is wrong."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            
            return response.choices[0].message.content or ""
            
        except Exception as e:
            print(f"LLM Error: {e}")
            return ""