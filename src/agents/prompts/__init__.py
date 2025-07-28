# src/agents/prompts/__init__.py
"""
Prompt templates for the schema-driven clarifier (two-phase).
"""

from .clarifier_prompt import (
    get_initial_clarifier_prompt,
    get_detailed_clarifier_prompt,
)

__all__ = [
    "get_initial_clarifier_prompt",
    "get_detailed_clarifier_prompt",
]
