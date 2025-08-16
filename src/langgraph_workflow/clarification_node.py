from typing import Dict, Any
from .state import PlanExecuteState

def clarification_node(state: PlanExecuteState, planner) -> Dict[str, Any]:
    """
    If the planner flagged ambiguities, surface them and (optionally) regenerate
    a clarified work-plan once answers arrive.

    Behaviour
    ─────────
    • first pass - no `clarification_answers` yet → ask questions & pause
    • second pass - answers present           → call planner again
    • otherwise - nothing to do            → fall through
    """
    if not state.get("requires_clarification", False):
        # nothing to do – proceed
        return {"clarification_needed": False}

    answers = state.get("clarification_answers")
    if not answers:
        # pause; the CLI (added in 4.3) will print these and collect replies
        print("\nℹ️  The system needs a few clarifications before it can continue:")
        for q in state["ambiguities"]:
            print(f"   • {q}")
        print("   (Reply with e.g.  --clarify '{\"Primary or Secondary …\":\"Primary Value\"}')")
        return {"clarification_needed": True}

    # we have answers → stitch them onto the original question and re-plan
    clarification_note = "\n".join([f"{k}: {v}" for k, v in answers.items()])
    clarified_query = state["input"] + "\n" + clarification_note

    new_plan = planner.plan(clarified_query)

    return {
        # replace the old plan
        "workplan": [s.dict() for s in new_plan.workplan],
        "current_step_index": 0,
        "step_results": {},
        "sql_queries": [],
        "ambiguities": new_plan.ambiguities,
        "requires_clarification": bool(new_plan.ambiguities),
        "clarification_needed": bool(new_plan.ambiguities),
    }
