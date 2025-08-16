# src/agents/executor.py
"""
Step Executor for executing individual workplan steps - Improved Version
Enhanced SQL generation and error handling
"""

from typing import Dict, Any, List, Optional
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
import json
import re
from sqlalchemy import text
import pandas as pd


class StepExecutionResult(BaseModel):
    """Result from executing a single step."""
    step_id: str
    success: bool
    sql: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    result_summary: Optional[str] = None


class StepExecutor:
    """
    Executes individual steps from the workplan.
    Converts each step into SQL and executes it.
    """
    
    def __init__(self, sql_agent, schema_adapter, db_connection):
        """
        Initialize the executor.
        
        Args:
            sql_agent: The existing AbbottSQLAgent for SQL generation
            schema_adapter: AbbottSchemaAdapter for schema information
            db_connection: Database connection for direct SQL execution
        """
        self.sql_agent = sql_agent
        self.schema_adapter = schema_adapter
        self.engine = db_connection
        self.conn = self.engine.connect()
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        
    def execute_step(self, step: Dict[str, Any], previous_results: Dict[str, Any]) -> StepExecutionResult:
        """
        Execute a single step from the workplan.
        
        Args:
            step: The step to execute
            previous_results: Results from previous steps
            
        Returns:
            StepExecutionResult with the execution outcome
        """
        step_id = step.get('id', 'unknown')
        step_type = step.get('type', 'unknown')
        
        try:
            # Build context from previous steps
            context = self._build_context(step, previous_results)
            
            # Generate SQL based on step type
            if step_type == 'filter':
                sql = self._generate_filter_sql(step, context)
            elif step_type == 'aggregate':
                sql = self._generate_aggregate_sql(step, context)
            elif step_type == 'calculate':
                sql = self._generate_calculate_sql(step, context)
            elif step_type == 'rank':
                sql = self._generate_rank_sql(step, context)
            elif step_type == 'compare':
                sql = self._generate_compare_sql(step, context)
            else:
                # Fallback: use the SQL agent for complex steps
                return self._execute_with_agent(step, context)
            
            # Execute the SQL
            if sql:
                print(f"Generated SQL for {step_id}:\n{sql}")
                result = self._execute_sql(sql)
                self._register_view(step_id, sql)
                summary = self._summarize_result(result, step)
                
                return StepExecutionResult(
                    step_id=step_id,
                    success=True,
                    sql=sql,
                    result=result,
                    result_summary=summary
                )
            else:
                # If no SQL generated, use the agent
                return self._execute_with_agent(step, context)
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            return StepExecutionResult(
                step_id=step_id,
                success=False,
                error=str(e)
            )
    
    def _build_context(self, step: Dict[str, Any], previous_results: Dict[str, Any]) -> Dict[str, Any]:
        """Build context from previous step results."""
        context = {
            'current_step': step,
            'previous_results': {}
        }
        
        # Add results from dependencies
        for dep_id in step.get('depends_on', []):
            if dep_id in previous_results:
                context['previous_results'][dep_id] = previous_results[dep_id]
        
        return context
    
    def _generate_filter_sql(self, step: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Generate SQL for filter steps."""
        params = step.get('params', {})
        question = step.get('question', '').lower()
        
        # Start with base table or previous step's result
        if step.get('depends_on'):
            # Use CTE from previous step
            dep_id = step['depends_on'][0]
            base = f"WITH {step['id']} AS (\n  SELECT * FROM {dep_id}\n  WHERE "
        else:
            base = f"WITH {step['id']} AS (\n  SELECT * FROM analyzer\n  WHERE "
        
        # Build WHERE conditions
        conditions = []
        
        # Always exclude Mth='All' unless specified
        if "'all'" not in question and "total" not in question:
            conditions.append("Mth != 'All'")
        
        # Add filter conditions from params
        for key, value in params.items():
            key_lower = key.lower()
            if key_lower == 'zone':
                # Handle zone filtering - check if it's in the Zone column
                conditions.append(f"UPPER(Zone) = UPPER('{value}')")
            elif key_lower == 'month':
                conditions.append(f"Mth = '{value}'")
            elif key_lower == 'status':
                conditions.append(f"Status = '{value}'")
            elif key_lower == 'territory':
                conditions.append(f"Terr_Code = '{value}'")
            elif key_lower == 'brand':
                conditions.append(f"Brand = '{value}'")
            elif key_lower == 'quarters' and isinstance(value, list):
                # Handle quarter filtering
                months = []
                for q in value:
                    if q.upper() == 'Q1':
                        months.extend(['Jan', 'Feb', 'Mar'])
                    elif q.upper() == 'Q2':
                        months.extend(['Apr', 'May', 'Jun'])
                    elif q.upper() == 'Q3':
                        months.extend(['Jul', 'Aug', 'Sep'])
                    elif q.upper() == 'Q4':
                        months.extend(['Oct', 'Nov', 'Dec'])
                if months:
                    quoted = ", ".join([f"'{m}'" for m in months])
                    conditions.append(f"Mth IN ({quoted})")
        
        # Handle specific cases from the question if params are incomplete
        if 'delhi' in question and not any('zone' in p.lower() for p in params):
            conditions.append("UPPER(Zone) = 'DELHI'")
        
        # Handle month names in question
        months_map = {
            'january': 'Jan', 'february': 'Feb', 'march': 'Mar', 'april': 'Apr',
            'may': 'May', 'june': 'Jun', 'july': 'Jul', 'august': 'Aug',
            'september': 'Sep', 'october': 'Oct', 'november': 'Nov', 'december': 'Dec',
            'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr',
            'jun': 'Jun', 'jul': 'Jul', 'aug': 'Aug', 'sep': 'Sep',
            'oct': 'Oct', 'nov': 'Nov', 'dec': 'Dec'
        }
        
        for month_name, month_code in months_map.items():
            if month_name in question and not any('month' in p.lower() for p in params):
                conditions.append(f"Mth = '{month_code}'")
                break
        
        if conditions:
            sql = base + ' AND '.join(conditions) + '\n)\nSELECT * FROM ' + step['id']
        else:
            # No conditions, just exclude All
            sql = f"WITH {step['id']} AS (\n  SELECT * FROM analyzer\n  WHERE Mth != 'All'\n)\nSELECT * FROM {step['id']}"
        
        return sql
    
    def _generate_aggregate_sql(self, step: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Generate SQL for aggregation steps."""
        params = step.get('params', {})
        question = step.get('question', '').lower()
        
        # Determine source table
        if step.get('depends_on'):
            source = step['depends_on'][0]
        else:
            source = 'analyzer'
        
        # Determine what to aggregate
        select_parts = []
        group_by_parts = []
        
        # Check what dimensions to include based on the question
        if 'by brand' in question or 'brand' in params.get('group_by', ''):
            select_parts.append('Brand')
            group_by_parts.append('Brand')
        if 'by territory' in question or 'territory' in params.get('group_by', ''):
            select_parts.append('Terr_Code')
            select_parts.append('TBM_Name')
            group_by_parts.extend(['Terr_Code', 'TBM_Name'])
        if 'by zone' in question or 'zone' in params.get('group_by', ''):
            select_parts.append('Zone')
            group_by_parts.append('Zone')
        if 'by status' in question or 'status' in params.get('group_by', ''):
            select_parts.append('Status')
            group_by_parts.append('Status')
        
        # If no grouping specified, check if we're looking at a specific zone/territory
        if not group_by_parts and ('delhi' in question or 'zone' in question):
            # Aggregate at zone level
            select_parts.append('Zone')
            group_by_parts.append('Zone')
        
        # Determine what metrics to calculate
        if 'primary' in question and 'value' in question:
            select_parts.append('SUM(Prim_Value) as total_primary_value')
            select_parts.append('SUM(Tgt_Value) as total_target_value')
        elif 'secondary' in question and 'value' in question:
            select_parts.append('SUM(Sec_Value) as total_secondary_value')
            select_parts.append('SUM(Tgt_Value) as total_target_value')
        elif 'primary' in question and 'unit' in question:
            select_parts.append('SUM(Prim_Units) as total_primary_units')
            select_parts.append('SUM(Tgt_Units) as total_target_units')
        elif 'secondary' in question and 'unit' in question:
            select_parts.append('SUM(Sec_Units) as total_secondary_units')
            select_parts.append('SUM(Tgt_Units) as total_target_units')
        elif 'target' in question or 'achievement' in question:
            # For achievement calculations, we need both actual and target
            select_parts.extend([
                'SUM(Prim_Value) as total_primary_value',
                'SUM(Tgt_Value) as total_target_value'
            ])
        else:
            # Default: include primary value and target for achievement calculation
            select_parts.extend([
                'SUM(Prim_Value) as total_primary_value',
                'SUM(Tgt_Value) as total_target_value'
            ])
        
        # Build SQL
        if group_by_parts:
            sql = f"WITH {step['id']} AS (\n"
            sql += f"  SELECT {', '.join(select_parts)}\n"
            sql += f"  FROM {source}\n"
            sql += f"  GROUP BY {', '.join(group_by_parts)}\n"
            sql += f")\nSELECT * FROM {step['id']}"
        else:
            # No grouping, just aggregation
            sql = f"WITH {step['id']} AS (\n"
            sql += f"  SELECT {', '.join(select_parts)}\n"
            sql += f"  FROM {source}\n"
            sql += f")\nSELECT * FROM {step['id']}"
        
        return sql
    
    def _generate_calculate_sql(self, step: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Generate SQL for calculation steps."""
        params = step.get('params', {})
        question = step.get('question', '').lower()
        metric = params.get('metric', '')
        
        # Determine source
        if step.get('depends_on'):
            source = step['depends_on'][0]
        else:
            source = 'analyzer'
        
        # Check if we're working with pre-aggregated data
        has_aggregated_columns = False
        if source != 'analyzer' and source in context.get('previous_results', {}):
            prev_result = context['previous_results'][source]
            if prev_result.get('sql', ''):
                # Check if previous step had aggregated columns
                prev_sql = prev_result['sql'].lower()
                has_aggregated_columns = 'sum(' in prev_sql or 'avg(' in prev_sql or 'count(' in prev_sql
        
        # Generate calculation based on metric or question
        if 'achievement' in question or 'achievement' in metric or 'achieve' in question or 'target' in question:
            # Determine which achievement to calculate
            if 'secondary' in question or 'secondary' in metric:
                if has_aggregated_columns:
                    calculation = "(total_secondary_value / NULLIF(total_target_value, 0)) * 100 as achievement_pct"
                else:
                    calculation = "(SUM(Sec_Value) / NULLIF(SUM(Tgt_Value), 0)) * 100 as achievement_pct"
            else:
                # Default to primary
                if has_aggregated_columns:
                    calculation = "(total_primary_value / NULLIF(total_target_value, 0)) * 100 as achievement_pct"
                else:
                    calculation = "(SUM(Prim_Value) / NULLIF(SUM(Tgt_Value), 0)) * 100 as achievement_pct"
        elif 'growth' in question or 'growth' in metric:
            if 'primary' in question or 'primary' in metric:
                calculation = "((SUM(Prim_Value) - SUM(LY_Prim_Value)) / NULLIF(SUM(LY_Prim_Value), 0)) * 100 as growth_pct"
            else:
                calculation = "((SUM(Sec_Value) - SUM(LY_Sec_Value)) / NULLIF(SUM(LY_Sec_Value), 0)) * 100 as growth_pct"
        elif 'gap' in question or 'gap' in metric:
            if has_aggregated_columns:
                calculation = "total_target_value - total_primary_value as performance_gap"
            else:
                calculation = "SUM(Tgt_Value) - SUM(Prim_Value) as performance_gap"
        else:
            # Default calculation
            if has_aggregated_columns:
                calculation = "total_primary_value as value"
            else:
                calculation = "SUM(Prim_Value) as value"
        
        # Build SQL based on whether we have pre-aggregated data
        if has_aggregated_columns:
            # Working with already aggregated data
            sql = f"WITH {step['id']} AS (\n  SELECT *, {calculation}\n  FROM {source}\n)\nSELECT * FROM {step['id']}"
        else:
            # Need to aggregate
            # Determine grouping
            group_by = []
            select_dims = []
            
            # Check if there are dimensional columns to preserve
            for col in ['Zone', 'Brand', 'Terr_Code', 'TBM_Name', 'Status']:
                if col.lower() in question or col in str(context.get('previous_results', {})):
                    select_dims.append(col)
                    group_by.append(col)
            
            if group_by:
                sql = f"WITH {step['id']} AS (\n  SELECT {', '.join(select_dims)}, {calculation}\n  FROM {source}"
                if source == 'analyzer':
                    sql += "\n  WHERE Mth != 'All'"
                sql += f"\n  GROUP BY {', '.join(group_by)}\n)\nSELECT * FROM {step['id']}"
            else:
                # No grouping
                sql = f"WITH {step['id']} AS (\n  SELECT {calculation}\n  FROM {source}"
                if source == 'analyzer':
                    sql += "\n  WHERE Mth != 'All'"
                sql += "\n)\nSELECT * FROM {step['id']}"
        
        return sql
    
    def _generate_rank_sql(self, step: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Generate SQL for ranking/sorting steps."""
        params = step.get('params', {})
        question = step.get('question', '').lower()
        
        # Determine source
        if step.get('depends_on'):
            source = step['depends_on'][0]
        else:
            source = 'analyzer'
        
        # Determine sort column based on previous steps or question
        order_by = None
        
        # Check what was calculated in previous steps
        if source in context.get('previous_results', {}):
            prev_sql = context['previous_results'][source].get('sql', '').lower()
            if 'achievement_pct' in prev_sql:
                order_by = 'achievement_pct'
            elif 'growth_pct' in prev_sql:
                order_by = 'growth_pct'
            elif 'total_secondary_value' in prev_sql:
                order_by = 'total_secondary_value'
            elif 'total_primary_value' in prev_sql:
                order_by = 'total_primary_value'
            elif 'performance_gap' in prev_sql:
                order_by = 'performance_gap'
        
        # If not found, determine from question
        if not order_by:
            if 'achievement' in question:
                order_by = 'achievement_pct'
            elif 'growth' in question:
                order_by = 'growth_pct'
            elif 'gap' in question:
                order_by = 'performance_gap'
            elif 'value' in question or 'sales' in question:
                if 'secondary' in question:
                    order_by = 'total_secondary_value'
                else:
                    order_by = 'total_primary_value'
            else:
                # Default to any numeric column
                order_by = 'total_primary_value'
        
        # Determine direction and limit
        if 'top' in question or 'highest' in question or 'best' in question:
            direction = 'DESC'
        elif 'bottom' in question or 'lowest' in question or 'worst' in question or 'underperform' in question:
            direction = 'ASC'
        else:
            direction = 'DESC'
        
        # Extract limit
        limit = 5  # default
        numbers = re.findall(r'\d+', question)
        if numbers:
            limit = int(numbers[0])
        
        # Build SQL
        sql = f"WITH {step['id']} AS (\n"
        sql += f"  SELECT *\n"
        sql += f"  FROM {source}\n"
        sql += f"  ORDER BY {order_by} {direction}\n"
        sql += f"  LIMIT {limit}\n"
        sql += f")\nSELECT * FROM {step['id']}"
        
        return sql
    
    def _generate_compare_sql(self, step: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Generate SQL for comparison steps."""
        # For complex comparisons, fall back to agent
        return None
    
    def _execute_with_agent(self, step: Dict[str, Any], context: Dict[str, Any]) -> StepExecutionResult:
        """Fallback: use the SQL agent for complex steps."""
        step_id = step.get('id', 'unknown')
        
        try:
            # Build a focused query for this step
            query = step.get('question', '')
            
            # Add context from previous steps
            if step.get('depends_on'):
                query += "\n\nContext from previous steps:"
                for dep_id in step['depends_on']:
                    if dep_id in context['previous_results']:
                        prev_result = context['previous_results'][dep_id]
                        if 'result_summary' in prev_result:
                            query += f"\n- {dep_id}: {prev_result['result_summary']}"
            
            print(f"Using SQL agent for step {step_id} with query: {query}")
            
            # Use the SQL agent
            result = self.sql_agent.ask(query)
            
            if result['success']:
                return StepExecutionResult(
                    step_id=step_id,
                    success=True,
                    sql=result.get('sql'),
                    result=result.get('result'),
                    result_summary=self._extract_summary(result.get('result'))
                )
            else:
                return StepExecutionResult(
                    step_id=step_id,
                    success=False,
                    error=result.get('error', 'Unknown error from SQL agent')
                )
                
        except Exception as e:
            import traceback
            traceback.print_exc()
            return StepExecutionResult(
                step_id=step_id,
                success=False,
                error=str(e)
            )
    
    def _execute_sql(self, sql: str) -> Any:
        """Execute SQL and return results."""
        try:
            result = self.conn.execute(text(sql))
            rows = [dict(r) for r in result.mappings().all()]
            return rows
        except Exception as e:
            raise Exception(f"SQL execution error: {str(e)}\nSQL: {sql}")
    
    def _register_view(self, view_name: str, sql: str) -> None:
        """
        Materialise the step's SELECT as a TEMP VIEW so later steps can
        reference it (e.g. `FROM step_1`).
        """
        try:
            # Extract the inner SELECT from the CTE
            pattern = rf"WITH\s+{re.escape(view_name)}\s+AS\s*\(\s*(.*?)\s*\)\s*SELECT"
            m = re.search(pattern, sql, flags=re.IGNORECASE | re.DOTALL)
            if m:
                inner = m.group(1).strip()
                create_view_sql = f"CREATE OR REPLACE TEMP VIEW {view_name} AS {inner}"
                self.conn.execute(text(create_view_sql))
                print(f"Created temp view: {view_name}")
        except Exception as e:
            print(f"Warning: Could not create view {view_name}: {e}")
            # Don't fail the whole run if view creation has issues
    
    def _summarize_result(self, result: Any, step: Dict[str, Any]) -> str:
        """Create a summary of the result for context."""
        if not result:
            return "No results"
        
        if isinstance(result, list) and len(result) > 0:
            count = len(result)
            if count == 1 and isinstance(result[0], dict):
                # Single row result - format nicely
                row = result[0]
                parts = []
                for k, v in row.items():
                    if v is not None:
                        if isinstance(v, (int, float)):
                            if 'pct' in k or 'percentage' in k:
                                parts.append(f"{k}: {v:.2f}%")
                            else:
                                parts.append(f"{k}: {v:,.0f}")
                        else:
                            parts.append(f"{k}: {v}")
                return "Result: " + ", ".join(parts)
            else:
                # Multiple rows
                return f"{count} rows returned"
        else:
            return str(result)
    
    def _extract_summary(self, result_text: str) -> str:
        """Extract a summary from the agent's result text."""
        if not result_text:
            return "No result"
        
        # Take first line or up to 200 characters
        lines = result_text.strip().split('\n')
        if lines:
            return lines[0][:200]
        return result_text[:200]