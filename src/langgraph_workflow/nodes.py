# src/langgraph_workflow/nodes.py
"""
Node implementations for the workflow.
"""

from typing import Dict, Any
from .state import PlanExecuteState


def planning_node(state: PlanExecuteState, planner) -> Dict[str, Any]:
    """Create a workplan for the query and detect ambiguities."""
    try:
        print(f"Planning for query: {state['input']}")
        plan_output = planner.plan(state["input"])

        workplan = [step.dict() for step in plan_output.workplan]
        ambiguities = plan_output.ambiguities or []
        if ambiguities:
            print("⚠️  Ambiguities detected that need user clarification:")
            for a in ambiguities:
                print(f"   • {a}")

        return {
            "workplan": workplan,
            "current_step_index": 0,
            "step_results": {},
            "sql_queries": [],
            "success": True,
            "ambiguities": ambiguities,
            "requires_clarification": bool(ambiguities),
            "clarification_answers": state.get("clarification_answers", {}),  # Fixed: use existing or empty dict
            "clarification_needed": bool(ambiguities)  # Fixed: pass the actual boolean value
        }

    except Exception as e:
        print(f"Planning error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "workplan": [],
            "success": False,
            "error": f"Planning failed: {str(e)}"
        }


def execute_step_node(state: PlanExecuteState, executor) -> Dict[str, Any]:
    """
    Execute the current step in the workplan.
    """
    workplan = state.get("workplan", [])
    current_index = state.get("current_step_index", 0)
    step_results = state.get("step_results", {})
    
    if current_index >= len(workplan):
        return {"success": True}  # All steps completed
    
    # Get current step
    current_step = workplan[current_index]
    step_id = current_step.get('id', f'step_{current_index}')
    
    print(f"\nExecuting Step {current_index + 1}/{len(workplan)}: {current_step.get('question', 'No description')}")
    
    try:
        # Execute the step
        result = executor.execute_step(current_step, step_results)
        
        # Store the result
        step_results[step_id] = result.dict()
        
        # Collect SQL queries
        sql_queries = state.get("sql_queries", [])
        if result.sql:
            sql_queries.append(result.sql)
        
        # Update state
        return {
            "past_steps": [(step_id, result.dict())],
            "current_step_index": current_index + 1,
            "step_results": step_results,
            "sql_queries": sql_queries,
            "success": result.success,
            "error": result.error if not result.success else None
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        
        return {
            "past_steps": [(step_id, {"error": str(e)})],
            "current_step_index": current_index + 1,
            "step_results": step_results,
            "success": False,
            "error": f"Step execution failed: {str(e)}"
        }


def aggregate_results_node(state: PlanExecuteState) -> Dict[str, Any]:
    """
    Aggregate results from all steps into a final response.
    """
    workplan = state.get("workplan", [])
    step_results = state.get("step_results", {})
    sql_queries = state.get("sql_queries", [])
    
    # Build final response
    response = "=" * 60 + "\n"
    response += "EXECUTION COMPLETE\n"
    response += "=" * 60 + "\n\n"
    
    # Show workplan execution summary
    response += "Workplan Execution Summary:\n"
    response += "-" * 30 + "\n"
    
    for i, step in enumerate(workplan):
        step_id = step.get('id', f'step_{i}')
        step_result = step_results.get(step_id, {})
        
        response += f"\nStep {i+1}: {step.get('question', 'No description')}\n"
        if step_result.get('success'):
            response += f"  ✓ Success\n"
            if step_result.get('result_summary'):
                response += f"  Result: {step_result['result_summary']}\n"
        else:
            response += f"  ✗ Failed: {step_result.get('error', 'Unknown error')}\n"
    
    # Show final result from last step
    response += "\n" + "=" * 60 + "\n"
    response += "FINAL RESULT:\n"
    response += "=" * 60 + "\n\n"
    
    # Get the last successful step's result
    last_result = None
    for step_id in reversed([s.get('id', f'step_{i}') for i, s in enumerate(workplan)]):
        if step_id in step_results and step_results[step_id].get('success'):
            last_result = step_results[step_id]
            break
    
    if last_result and last_result.get('result'):
        result_data = last_result['result']
        if isinstance(result_data, list) and len(result_data) > 0:
            # Format as table
            if isinstance(result_data[0], dict):
                # Get columns
                columns = list(result_data[0].keys())
                response += " | ".join(columns) + "\n"
                response += "-" * (len(" | ".join(columns))) + "\n"
                
                # Show rows (limit to 20 for readability)
                for row in result_data[:20]:
                    values = [str(row.get(col, '')) for col in columns]
                    response += " | ".join(values) + "\n"
                
                if len(result_data) > 20:
                    response += f"\n... and {len(result_data) - 20} more rows\n"
            else:
                response += str(result_data)
        else:
            response += str(result_data)
    else:
        response += "No final result available.\n"
    
    # Show all SQL queries executed
    if sql_queries:
        response += "\n" + "=" * 60 + "\n"
        response += "SQL QUERIES EXECUTED:\n"
        response += "=" * 60 + "\n"
        for i, sql in enumerate(sql_queries, 1):
            response += f"\n-- Step {i} SQL:\n{sql}\n"
    
    # Determine the final SQL (could be a combined query or the last one)
    final_sql = sql_queries[-1] if sql_queries else None
    
    return {
        "final_response": response,
        "sql_query": final_sql,
        "success": all(r.get('success', False) for r in step_results.values()) if step_results else False
    }

def format_response_node(state: PlanExecuteState) -> Dict[str, Any]:
    """
    Format the final response for the user.
    """
    # In Phase 3 aggregation already produced the final text
    return {
        "final_response": state.get("final_response", "Query processing complete.")
    }