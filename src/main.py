import click
import json
from pathlib import Path
from langchain_sql.sql_agent import AbbottSQLAgent
from langchain_sql.validation import SQLValidator
from langchain_sql.schema_adapter import AbbottSchemaAdapter

# Default paths and model settings are now hard-coded.
DEFAULT_DB_PATH = 'local.duckdb'
DEFAULT_YAML_PATH = 'registry/semantic_layer/analyzer.yaml'
DEFAULT_MODEL = 'gpt-4o-mini'

@click.group()
def cli():
    """Abbott AI Analysis Tool - Natural Language to SQL Interface"""
    pass

@cli.command()
@click.argument('question')
def ask(question):
    """Ask a natural language question about the sales data."""
    # Check if the required files exist
    db_path = Path(DEFAULT_DB_PATH)
    yaml_path = Path(DEFAULT_YAML_PATH)
    if not db_path.exists():
        click.echo(f"Error: Database file '{DEFAULT_DB_PATH}' not found", err=True)
        return
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return

    # Initialize agent using default paths and model
    click.echo("Initializing Abbott SQL Agent...")
    try:
        agent = AbbottSQLAgent(DEFAULT_DB_PATH, DEFAULT_YAML_PATH, DEFAULT_MODEL)
    except Exception as e:
        click.echo(f"Error initializing agent: {e}", err=True)
        return

    # Process the question
    click.echo(f"\nQuestion: {question}")
    click.echo("Processing...\n")
    
    result = agent.ask(question)
    
    if result['success']:
        # Show SQL
        click.echo(click.style("Generated SQL:", fg='green', bold=True))
        click.echo(result['sql'])
        click.echo()
        
        # Validate the generated SQL if available
        if result['sql']:
            validator = SQLValidator(agent.schema_adapter)
            validation = validator.validate(result['sql'])
            
            if not validation['valid']:
                click.echo(click.style("Validation Issues:", fg='yellow', bold=True))
                for issue in validation['issues']:
                    click.echo(f"  - {issue}")
                click.echo()
            if validation.get('warnings'):
                click.echo(click.style("Warnings:", fg='yellow'))
                for warning in validation['warnings']:
                    click.echo(f"  - {warning}")
                click.echo()
        
        # Show results
        click.echo(click.style("Results:", fg='green', bold=True))
        click.echo(result['result'])
       
        # Optional: Show intermediate steps if running in a verbose context
        ctx = click.get_current_context()
        if ctx.obj and ctx.obj.get('verbose'):
            click.echo("\nIntermediate Steps:")
            for i, step in enumerate(result.get('intermediate_steps', [])):
                click.echo(f"Step {i+1}: {step}")
       
    else:
        # Show error
        click.echo(click.style(f"Error: {result['error']}", fg='red', bold=True))
        click.echo(f"Error Type: {result.get('error_type', 'Unknown')}")

@cli.command()
@click.argument('sql_query')
def validate(sql_query):
    """Validate a SQL query against Abbott business rules."""
    yaml_path = Path(DEFAULT_YAML_PATH)
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return

    adapter = AbbottSchemaAdapter(str(yaml_path))
    validator = SQLValidator(adapter)
    result = validator.validate(sql_query)
   
    if result['valid']:
        click.echo(click.style("✓ SQL is valid", fg='green', bold=True))
    else:
        click.echo(click.style("✗ SQL has issues:", fg='red', bold=True))
        for issue in result['issues']:
            click.echo(f"  - {issue}")
   
    if result.get('warnings'):
        click.echo(click.style("\nWarnings:", fg='yellow'))
        for warning in result['warnings']:
            click.echo(f"  - {warning}")
   
    click.echo("\nFormatted SQL:")
    click.echo(result['formatted_sql'])

@cli.command()
def interactive():
    """Start an interactive session for asking multiple questions."""
    db_path = Path(DEFAULT_DB_PATH)
    yaml_path = Path(DEFAULT_YAML_PATH)
    if not db_path.exists():
        click.echo(f"Error: Database file '{DEFAULT_DB_PATH}' not found", err=True)
        return
    if not yaml_path.exists():
        click.echo(f"Error: Schema file '{DEFAULT_YAML_PATH}' not found", err=True)
        return

    click.echo("Initializing Abbott SQL Agent...")
    try:
        agent = AbbottSQLAgent(DEFAULT_DB_PATH, DEFAULT_YAML_PATH, DEFAULT_MODEL)
        validator = SQLValidator(agent.schema_adapter)
    except Exception as e:
        click.echo(f"Error initializing agent: {e}", err=True)
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
        result = agent.ask(question)
       
        if result['success']:
            click.echo(click.style("\nGenerated SQL:", fg='green'))
            click.echo(result['sql'])
           
            if result['sql']:
                validation = validator.validate(result['sql'])
                if not validation['valid']:
                    click.echo(click.style("\nValidation Issues:", fg='yellow'))
                    for issue in validation['issues']:
                        click.echo(f"  - {issue}")
           
            click.echo(click.style("\nResults:", fg='green'))
            click.echo(result['result'])
        else:
            click.echo(click.style(f"\nError: {result['error']}", fg='red'))

if __name__ == '__main__':
    cli()