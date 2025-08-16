# src/langgraph_workflow/workflow.py
"""
Main workflow implementation using LangGraph.
"""

from typing import Dict, Any, Optional
from langgraph.graph import StateGraph, END
from .state import PlanExecuteState
from .nodes import planning_node, execute_step_node, aggregate_results_node, format_response_node
from ..langchain_sql.sql_agent import AbbottSQLAgent
from ..langchain_sql.schema_adapter import AbbottSchemaAdapter
from ..agents.planner import SimplePlanner
from ..agents.executor import StepExecutor
from .clarification_node import clarification_node


class AbbottPlanExecuteWorkflow:
    """
    Plan-and-execute workflow for complex SQL queries.
    Phase 4: With clarification support.
    """
    
    def __init__(self, db_path: str, yaml_path: str, model: str = "gpt-4o-mini"):
        """Initialize the workflow with database and schema paths."""
        self.db_path = db_path
        self.yaml_path = yaml_path
        self.model = model
        
        # Initialize components
        self.schema_adapter = AbbottSchemaAdapter(yaml_path)
        self.sql_agent = AbbottSQLAgent(db_path, yaml_path, model)
        self.planner = SimplePlanner(self.schema_adapter, model)
        
        # Initialize executor with database connection
        self.executor = StepExecutor(
            sql_agent=self.sql_agent,
            schema_adapter=self.schema_adapter,
            db_connection=self.sql_agent.engine
        )
        
        # Build the workflow graph
        self.app = self._build_workflow()
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow."""
        # Create the graph with our state type
        workflow = StateGraph(PlanExecuteState)
        
        # Add nodes with bound functions
        workflow.add_node("plan", lambda state: planning_node(state, self.planner))
        workflow.add_node("clarify", lambda s: clarification_node(s, self.planner))
        workflow.add_node("execute_step", lambda state: execute_step_node(state, self.executor))
        workflow.add_node("aggregate", aggregate_results_node)
        workflow.add_node("format_response", format_response_node)
        
        # Set entry point
        workflow.set_entry_point("plan")
        
        # Add edges
        workflow.add_edge("plan", "clarify")
        
        # Conditional edge after clarification
        def after_clarify(state: PlanExecuteState) -> str:
            if state.get("clarification_needed", False):
                # Clarifications needed and no answers yet - end the workflow
                return END
            return "execute_step"
        
        workflow.add_conditional_edges(
            "clarify",
            after_clarify,
            {
                "execute_step": "execute_step",
                END: END
            }
        )
        
        # Conditional edge after execute_step
        def after_execute(state: PlanExecuteState) -> str:
            current_index = state.get("current_step_index", 0)
            workplan = state.get("workplan", [])
            
            if current_index >= len(workplan):
                # All steps completed
                return "aggregate"
            else:
                # More steps to execute
                return "execute_step"
        
        workflow.add_conditional_edges(
            "execute_step",
            after_execute,
            {
                "execute_step": "execute_step",
                "aggregate": "aggregate"
            }
        )
        
        workflow.add_edge("aggregate", "format_response")
        workflow.add_edge("format_response", END)
        
        # Compile the workflow
        return workflow.compile()
    
    def run(self, question: str, clarification_answers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Run the workflow with a question.
        
        Args:
            question: Natural language question
            clarification_answers: Optional pre-filled clarification answers
            
        Returns:
            Dictionary with final_response and other details
        """
        initial_state = {
            "input": question,
            "workplan": [],
            "past_steps": [],
            "current_step_index": 0,
            "step_results": {},
            "sql_queries": [],
            "final_response": "",
            "success": False,
            "sql_query": None,
            "error": None,
            "ambiguities": [],
            "requires_clarification": False,
            "clarification_answers": clarification_answers or {},
            "clarification_needed": False
        }
        
        # Run the workflow
        result = self.app.invoke(initial_state)
        
        return result