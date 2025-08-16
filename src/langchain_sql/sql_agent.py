from typing import Dict, Any, Optional
from langchain_community.agent_toolkits import create_sql_agent, SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.agents.agent_types import AgentType
from sqlalchemy import create_engine
import re
from .schema_adapter import AbbottSchemaAdapter
from .few_shot_examples import AbbottFewShotExamples
from .custom_prompts import get_abbott_sql_prompt

import warnings
from duckdb_engine import DuckDBEngineWarning



import os
# Completely disable LangSmith tracing
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_ENDPOINT"] = ""
os.environ["LANGCHAIN_API_KEY"] = ""
os.environ["LANGCHAIN_PROJECT"] = ""


from langchain.globals import set_debug, set_verbose
set_debug(False)
set_verbose(False)

# Suppress DuckDB reflection warnings
warnings.filterwarnings("ignore", category=DuckDBEngineWarning)


class AbbottSQLAgent:
    """Main SQL Agent for Abbott pharmaceutical sales analytics"""
    
    def __init__(self, db_path: str, yaml_path: str, llm_model: str = "gpt-4o-mini"):
        """
        Initialize the Abbott SQL Agent.
        
        Args:
            db_path: Path to DuckDB database file
            yaml_path: Path to analyzer.yaml schema file
            llm_model: OpenAI model to use (default: gpt-4o-mini)
        """
        # Initialize schema adapter
        self.schema_adapter = AbbottSchemaAdapter(yaml_path)
        
        # Create DuckDB connection
        self.engine = create_engine(f"duckdb:///{db_path}")
        
        # Get table name from schema
        table_name = self.schema_adapter.get_table_name()
        
        # Initialize SQLDatabase with custom table info
        self.db = SQLDatabase(
            self.engine,
            include_tables=[table_name],
            sample_rows_in_table_info=3,  # Include 3 sample rows for context
            custom_table_info=self.schema_adapter.get_custom_table_info()
        )
        
        # Initialize LLM with fixed model_kwargs
        self.llm = ChatOpenAI(
            model=llm_model,
            temperature=0,  # Deterministic for SQL generation
            seed=42  # For reproducibility - moved from model_kwargs
        )
        
        # Get examples and create custom prompt
        self.examples = AbbottFewShotExamples.get_examples()
        self.prompt = get_abbott_sql_prompt(self.examples, self.schema_adapter)
        
    def create_agent(self) -> Any:
        """
        Create the SQL agent with Abbott-specific configuration.
        
        Returns:
            Configured LangChain SQL agent
        """
        # Create SQL toolkit with our database and LLM
        toolkit = SQLDatabaseToolkit(
            db=self.db,
            llm=self.llm
        )
        
        # Create agent with custom configuration
        return create_sql_agent(
            llm=self.llm,
            toolkit=toolkit,
            prompt=self.prompt,
            agent_type=AgentType.OPENAI_FUNCTIONS,  # Best for structured outputs
            verbose=True,  # Show reasoning steps
            top_k=10,  # Limit rows returned in queries
            handle_parsing_errors=True,  # Gracefully handle errors
            max_iterations=5,  # Prevent infinite loops
            early_stopping_method="force",  
            return_intermediate_steps=True  # Return SQL and reasoning
        )

    def ask(self, question: str) -> Dict[str, Any]:
        """
        Process a natural language question about sales data.
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with results or error information
        """

        # Create agent instance
        agent = self.create_agent()
        
        # Add context to question if needed
        enhanced_question = self._enhance_question(question)
        
        try:
            # Execute agent with question
            result = agent.invoke({"input": enhanced_question})
            
            # Extract SQL from intermediate steps
            sql_query = self._extract_sql(result)
            
            return {
                "success": True,
                "question": question,
                "enhanced_question": enhanced_question,
                "sql": sql_query,
                "result": result.get("output", "No output generated"),
                "intermediate_steps": result.get("intermediate_steps", [])
            }
            
        except Exception as e:
            return {
                "success": False,
                "question": question,
                "enhanced_question": enhanced_question,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _enhance_question(self, question: str) -> str:
        """
        Enhance question with business context if needed.
        
        Args:
            question: Original question
            
        Returns:
            Enhanced question with additional context
        """
        # Get business vocabulary
        business_context = self.schema_adapter.get_business_context()
        vocabulary = business_context.get('vocabulary', {})
        
        # Simple enhancement - add clarifications for ambiguous terms
        enhanced = question
        
        # Check for ambiguous terms
        if 'sales' in question.lower() and 'primary' not in question.lower() and 'secondary' not in question.lower():
            enhanced += " (Note: 'sales' typically means Primary Value unless specified)"
        
        if 'achievement' in question.lower() and '%' not in question:
            enhanced += " (Show achievement as percentage)"
        
        return enhanced
    
    def _extract_sql(self, result: Dict[str, Any]) -> Optional[str]:
        """
        Extract SQL query from agent results.
        
        Args:
            result: Agent execution result
            
        Returns:
            Extracted SQL query or None
        """
        # Look through intermediate steps for SQL queries
        intermediate_steps = result.get('intermediate_steps', [])
        print(f"DEBUG: Found {len(intermediate_steps)} intermediate steps")
        
        # Keep track of the last SQL query found
        last_sql = None
        
        for i, step in enumerate(intermediate_steps):
            if len(step) >= 2:
                action = step[0]
                result_text = step[1]
                
                # Debug print
                if hasattr(action, 'tool'):
                    print(f"DEBUG: Step {i} - Tool: {action.tool}")
                
                # Check various SQL-related tools
                if hasattr(action, 'tool'):
                    tool_name = action.tool.lower()
                    if any(sql_tool in tool_name for sql_tool in ['sql', 'query', 'database']):
                        if hasattr(action, 'tool_input'):
                            tool_input = action.tool_input
                            
                            # Handle different input formats
                            if isinstance(tool_input, str):
                                # Direct SQL string
                                if tool_input.strip():
                                    last_sql = tool_input.strip()
                                    print(f"DEBUG: Found SQL in step {i}: {last_sql[:100]}...")
                            elif isinstance(tool_input, dict):
                                # Dictionary with query key
                                for key in ['query', 'sql', 'input', 'sql_query']:
                                    if key in tool_input and tool_input[key]:
                                        last_sql = tool_input[key].strip()
                                        print(f"DEBUG: Found SQL in step {i} under key '{key}': {last_sql[:100]}...")
                                        break
        
        # If we found SQL in intermediate steps, return it
        if last_sql:
            return last_sql
        
        # Fallback: try to extract SQL from output using regex
        output = result.get('output', '')
        
        # Try different SQL block patterns
        patterns = [
            r'```sql\n(.*?)\n```',
            r'```SQL\n(.*?)\n```',
            r'```\n(SELECT.*?)\n```',
            r'SQL:\n(.*?)(?:\n\n|\Z)',
            r'query:\s*["\']?(SELECT.*?)["\']\s*[,}]'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, output, re.DOTALL | re.IGNORECASE)
            if matches:
                sql = matches[-1].strip()  # Get the last match
                print(f"DEBUG: SQL extracted from output using pattern: {sql[:100]}...")
                return sql
        
        print("DEBUG: No SQL found in result")
        return None
    
    def validate_sql(self, sql: str) -> Dict[str, Any]:
        """
        Validate SQL query against Abbott business rules.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Validation result with any issues found
        """
        issues = []
        
        # Check for Mth = 'All' exclusion
        if "mth != 'all'" not in sql.lower() and "mth <> 'all'" not in sql.lower():
            if "'all'" in sql.lower():
                issues.append("Query might include Mth = 'All' aggregate rows")
        
        # Check for division by zero protection
        if "/" in sql and "nullif" not in sql.lower():
            issues.append("Division operations should use NULLIF to prevent division by zero")
        
        # Check for NULL handling in calculations
        if any(op in sql for op in ['+', '-', '*', '/']) and "coalesce" not in sql.lower():
            issues.append("Consider using COALESCE for NULL value handling in calculations")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "sql": sql
        }