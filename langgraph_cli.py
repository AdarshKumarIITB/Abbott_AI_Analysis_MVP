#!/usr/bin/env python
"""
LangGraph CLI with clarification support - Phase 4.3
Run this directly: python langgraph_cli.py ask "your question"
"""

import click
import json
from pathlib import Path
from typing import Dict, Any, Optional
import sys
import os

# Add the parent directory to the path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.langgraph_workflow import AbbottPlanExecuteWorkflow

# Default paths (same as main.py)
DEFAULT_DB_PATH = 'local.duckdb'
DEFAULT_YAML_PATH = 'registry/semantic_layer/analyzer.yaml'
DEFAULT_MODEL = 'gpt-4o-mini'


class InteractiveWorkflow:
    """Wrapper to handle clarification loops in the CLI."""
    
    def __init__(self, workflow: AbbottPlanExecuteWorkflow):
        self.workflow = workflow
        
    def run_with_clarifications(self, question: str, 
                               clarification_answers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Run the workflow, handling clarification loops.
        """
        # Initialize state with clarification support
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
        
        # Run workflow
        result = self.workflow.app.invoke(initial_state)
        
        # Check if clarifications are needed
        if result.get("clarification_needed", False) and result.get("ambiguities"):
            if not clarification_answers:
                # In interactive mode, collect answers
                click.echo("\n" + "="*60)
                click.echo(click.style("Clarifications Needed", fg='yellow', bold=True))
                click.echo("="*60)
                click.echo("\nI need some clarifications to better understand your query:\n")
                
                answers = {}
                for ambiguity in result["ambiguities"]:
                    click.echo(f"• {ambiguity}")
                    answer = click.prompt("  Your answer", type=str)
                    answers[ambiguity] = answer
                    click.echo()
                
                # Re-run with clarification answers
                return self.run_with_clarifications(question, answers)
            else:
                # We have answers, re-run the workflow with them
                initial_state["clarification_answers"] = clarification_answers
                result = self.workflow.app.invoke(initial_state)
        
        return result


@click.group()
def cli():
    """Abbott AI Analysis Tool - LangGraph Plan & Execute Interface"""
    pass


@cli.command()
@click.argument('question')
@click.option('--clarify', type=str, help='JSON string of clarification answers for non-interactive mode')
def ask(question, clarify):
    """Ask a natural language question about the sales data."""
    # Check if required files exist
    db_path = Path(DEFAULT_DB_PATH)
    yaml_path = Path(DEFAULT_YAML_PATH)
    
    if not db_path.exists():
        click.echo(f"Error: Database file '{DEFAULT_DB_PATH}' not found", err=True)
        return
    
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return
    
    # Initialize workflow
    click.echo("Initializing LangGraph workflow...")
    try:
        workflow = AbbottPlanExecuteWorkflow(
            db_path=DEFAULT_DB_PATH,
            yaml_path=DEFAULT_YAML_PATH,
            model=DEFAULT_MODEL
        )
        interactive = InteractiveWorkflow(workflow)
    except Exception as e:
        click.echo(f"Error initializing workflow: {e}", err=True)
        return
    
    # Parse clarification answers if provided
    clarification_answers = {}
    if clarify:
        try:
            clarification_answers = json.loads(clarify)
        except json.JSONDecodeError:
            click.echo(f"Error: Invalid JSON in --clarify option", err=True)
            return
    
    # Process the question
    click.echo(f"\nQuestion: {question}")
    click.echo("Processing...\n")
    
    try:
        result = interactive.run_with_clarifications(question, clarification_answers)
        
        # Display results
        if result.get('success'):
            click.echo(result.get('final_response', 'No response generated'))
        else:
            click.echo(click.style(f"Error: {result.get('error', 'Unknown error')}", fg='red'))
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        traceback.print_exc()


@cli.command()
def interactive():
    """Start an interactive session for asking multiple questions."""
    # Check if required files exist
    db_path = Path(DEFAULT_DB_PATH)
    yaml_path = Path(DEFAULT_YAML_PATH)
    
    if not db_path.exists():
        click.echo(f"Error: Database file '{DEFAULT_DB_PATH}' not found", err=True)
        return
    
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return
    
    # Initialize workflow
    click.echo("Initializing LangGraph workflow...")
    try:
        workflow = AbbottPlanExecuteWorkflow(
            db_path=DEFAULT_DB_PATH,
            yaml_path=DEFAULT_YAML_PATH,
            model=DEFAULT_MODEL
        )
        interactive = InteractiveWorkflow(workflow)
    except Exception as e:
        click.echo(f"Error initializing workflow: {e}", err=True)
        return
    
    click.echo(click.style("\nAbbott AI Analysis - Interactive Mode", fg='blue', bold=True))
    click.echo("Type 'exit' or 'quit' to end the session")
    click.echo("Type 'help' for available commands")
    click.echo("-" * 50)
    
    while True:
        # Get user input
        question = click.prompt('\nQuestion', type=str)
        
        if question.lower() in ['exit', 'quit']:
            click.echo("Goodbye!")
            break
        
        if question.lower() == 'help':
            click.echo("\nAvailable commands:")
            click.echo("  exit/quit - End the session")
            click.echo("  help - Show this help")
            continue
        
        click.echo("\nProcessing...")
        
        try:
            result = interactive.run_with_clarifications(question)
            
            if result.get('success'):
                click.echo(result.get('final_response', 'No response generated'))
            else:
                click.echo(click.style(f"Error: {result.get('error', 'Unknown error')}", fg='red'))
                
        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            import traceback
            traceback.print_exc()


@cli.command()
def test():
    """Run automated tests with pre-defined clarification answers."""
    # Check if required files exist
    db_path = Path(DEFAULT_DB_PATH)
    yaml_path = Path(DEFAULT_YAML_PATH)
    
    if not db_path.exists():
        click.echo(f"Error: Database file '{DEFAULT_DB_PATH}' not found", err=True)
        return
    
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return
    
    # Initialize workflow
    click.echo("=== Testing Phase 4: Clarification Support ===\n")
    click.echo("Initializing workflow...")
    
    try:
        workflow = AbbottPlanExecuteWorkflow(
            db_path=DEFAULT_DB_PATH,
            yaml_path=DEFAULT_YAML_PATH,
            model=DEFAULT_MODEL
        )
        interactive = InteractiveWorkflow(workflow)
    except Exception as e:
        click.echo(f"Error initializing workflow: {e}", err=True)
        return
    
    # Test cases with pre-defined clarification answers
    test_cases = [
        {
            "query": "What is the total primary sales value for DELHI zone in Apr?",
            "clarifications": {
                "Primary or Secondary AND Value or Units?": "Primary Value"
            }
        },
        {
            "query": "Show me top 5 brands by secondary sales value",
            "clarifications": {
                "Primary or Secondary AND Value or Units?": "Secondary Value"
            }
        },
        {
            "query": "What are the sales for Delhi?",
            "clarifications": {
                "'sales' – is that Primary or Secondary?  Value or Units?": "Primary Value"
            }
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        query = test_case["query"]
        clarifications = test_case.get("clarifications", {})
        
        click.echo(f"\n{'='*80}")
        click.echo(f"Test {i}: {query}")
        click.echo('='*80)
        
        try:
            # Run with pre-filled clarifications
            result = interactive.run_with_clarifications(query, clarifications)
            
            # Display results
            click.echo(f"\nOverall Success: {result.get('success')}")
            
            if result.get('success'):
                click.echo("\n✓ Query executed successfully")
                # Show abbreviated response
                response_lines = result.get('final_response', '').split('\n')
                for line in response_lines[:10]:  # Show first 10 lines
                    click.echo(line)
                if len(response_lines) > 10:
                    click.echo("... (truncated)")
            else:
                click.echo(f"\n✗ Error: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            click.echo(f"Error: {e}", err=True)
            import traceback
            traceback.print_exc()
    
    click.echo("\n\n=== Phase 4 Testing Complete ===")


if __name__ == '__main__':
    cli()