# src/langgraph_workflow/state.py
"""
State definitions for the plan-and-execute workflow.
"""

from typing import List, Dict, Optional, Tuple, Annotated, Any
from typing_extensions import TypedDict
import operator
from pydantic import BaseModel, Field


class WorkplanStep(BaseModel):
    """Represents a single step in the execution plan."""
    id: str = Field(description="Unique identifier for this step")
    type: str = Field(description="Type of operation: filter, aggregate, rank, compare")
    depends_on: List[str] = Field(default_factory=list, description="IDs of steps this depends on")
    question: str = Field(description="Natural language description of what this step does")
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for this step")


class PlanExecuteState(TypedDict):
    """State for the plan-and-execute workflow."""
    # Input
    input: str  # Original user query
    
    # Planning (Phase 2)
    workplan: List[Dict[str, Any]]  # List of workplan steps
    
    # Execution (Phase 3)
    past_steps: Annotated[List[Tuple[str, Dict]], operator.add]  # Executed steps
    current_step_index: int  # Current step being executed
    step_results: Dict[str, Any]  # Results keyed by step ID
    
    # Results
    final_response: str
    sql_query: Optional[str]
    sql_queries: List[str]  # All SQL queries executed
    success: bool
    error: Optional[str]

    # Phase 4 - Clarification support
    ambiguities: List[str]                # any clarifications needed (may be empty)
    requires_clarification: bool          # True if ambiguities list is non-empty
    clarification_answers: Dict[str, str] # user replies keyed by question
    clarification_needed: bool            # helper flag for the router