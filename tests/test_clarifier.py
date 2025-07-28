"""
Smoke-test + interactive demo for the multi-hierarchy QueryClarifier
$ python tests/test_clarifier.py        # batch mode
$ python tests/test_clarifier.py i      # interactive mode
"""
import sys
from pathlib import Path
from pprint import pprint

# allow  import agents.*
sys.path.append(str(Path(__file__).parent.parent / "src"))

from agents.clarifier import QueryClarifier

qc = QueryClarifier()

# ──────────────────────────────────────────────────────────────
def batch() -> None:
    """Pre-defined assertions: fails if clarifier drifts."""
    cases = [
        ("Which brands are underperforming in North zone?",
         {"calculated_metrics": {"achievement_pct"},
          "dimension_hierarchies": {"geography", "product"},
          "granularity": {"geography": {"Zone"}, "product": {"Brand"}}}),

        ("Show me top 5 territories by sales in April",
         {"dimension_hierarchies": {"geography"},
          "granularity": {"geography": {"Terr_Code"}}}),

        ("Compare April primary sales with March for focus brands",
         {"metrics_needed": {"Prim_Value"},
          "filters": {"Status": "FOCUS"},
          "dimension_hierarchies": {"product"},
          "granularity": {"product": {"Brand"}}}),

        ("What's the YoY growth of each zone?",
         {"calculated_metrics": {"yoy_growth_pct"},
          "dimension_hierarchies": {"geography"},
          "granularity": {"geography": {"Zone"}}}),
    ]

    for idx, (query, expect) in enumerate(cases, 1):
        intent = qc.clarify(query)
        print(f"\n{idx}. {query}")
        pprint(intent.model_dump())

        # basic checks
        if "ambiguity" in expect:
            assert bool(intent.ambiguities) is expect["ambiguity"]

        if "dimension_hierarchies" in expect:
            assert expect["dimension_hierarchies"].issubset(
                set(intent.dimension_hierarchies) or set(intent.granularity)
            )

        if "granularity" in expect:
            for h, lvls in expect["granularity"].items():
                assert set(lvls).issubset(set(intent.granularity.get(h, [])))

        for key in ("metrics_needed", "calculated_metrics"):
            if key in expect:
                assert expect[key].issubset(set(getattr(intent, key)))

        for fk, fv in expect.get("filters", {}).items():
            assert intent.filters.get(fk) == fv

    print("\n✅  All batch checks passed.")

# ──────────────────────────────────────────────────────────────
def interactive() -> None:
    """One-question-at-a-time loop with optional follow-up handling."""
    while True:
        q = input("\nAsk > ").strip()
        if q.lower() in {"exit", "quit", ""}:
            break

        intent = qc.clarify(q)
        pprint(intent.model_dump())

        # If clarification needed, prompt the user once
        if intent.requires_clarification and intent.ambiguities:
            follow_up_q = intent.ambiguities[0].suggested_clarification
            follow_up = input(f"\nClarifier asks: {follow_up_q}\nYou > ").strip()
            intent = qc.refine_with_clarification(follow_up, intent)
            print("\nUpdated intent:")
            pprint(intent.model_dump())

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].startswith("i"):
        interactive()
    else:
        batch()
