from typing import Dict, List, Any
import re
import sqlparse

class SQLValidator:
    """Validator for Abbott-specific SQL queries"""
    
    def __init__(self, schema_adapter: Any):
        """
        Initialize validator with schema information.
        
        Args:
            schema_adapter: AbbottSchemaAdapter instance
        """
        self.schema_adapter = schema_adapter
        self.table_name = schema_adapter.get_table_name()
        
        # Get valid column names
        self.valid_columns = set()
        for col in schema_adapter.schema.get('columns', []):
            self.valid_columns.add(col['name'].lower())
    
    def validate(self, sql: str) -> Dict[str, Any]:
        """
        Comprehensive SQL validation for Abbott queries.
        
        Args:
            sql: SQL query string
            
        Returns:
            Validation results with any issues found
        """
        issues = []
        warnings = []
        
        # Format SQL for analysis
        formatted_sql = sqlparse.format(sql, reindent=True, keyword_case='upper')
        sql_lower = sql.lower()
        
        # 1. Check for Mth = 'All' exclusion
        if not self._excludes_all_month(sql_lower):
            issues.append("Missing Mth != 'All' filter - query may include aggregate rows")
        
        # 2. Check for division by zero protection
        if self._has_division(sql) and not self._has_nullif(sql_lower):
            issues.append("Division operations should use NULLIF(denominator, 0)")
        
        # 3. Check column validity
        invalid_cols = self._check_column_validity(sql)
        if invalid_cols:
            issues.append(f"Invalid columns: {', '.join(invalid_cols)}")
        
        # 4. Check for proper aggregation
        if 'group by' in sql_lower:
            agg_issues = self._check_aggregation(formatted_sql)
            issues.extend(agg_issues)
        
        # 5. Check for business logic
        business_warnings = self._check_business_logic(sql_lower)
        warnings.extend(business_warnings)
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings,
            "formatted_sql": formatted_sql
        }
    
    def _excludes_all_month(self, sql_lower: str) -> bool:
        """Check if query properly excludes Mth = 'All'"""
        patterns = [
            r"mth\s*!=\s*'all'",
            r"mth\s*<>\s*'all'",
            r"mth\s+not\s+in\s*\([^)]*'all'[^)]*\)",
            r"mth\s+in\s*\([^)]+\)"  # Specific month selection
        ]
        
        return any(re.search(pattern, sql_lower) for pattern in patterns)
    
    def _has_division(self, sql: str) -> bool:
        """Check if query contains division operations"""
        # Look for / not in comments
        lines = sql.split('\n')
        for line in lines:
            if '--' in line:
                line = line[:line.index('--')]
            if '/*' in line:
                continue
            if '/' in line:
                return True
        return False
    
    def _has_nullif(self, sql_lower: str) -> bool:
        """Check if query uses NULLIF for safe division"""
        return 'nullif' in sql_lower
    
    def _check_column_validity(self, sql: str) -> List[str]:
        """Check for invalid column references"""
        # Extract potential column names using regex
        # This is simplified - production would use proper SQL parsing
        column_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        potential_columns = re.findall(column_pattern, sql)
        
        invalid = []
        sql_keywords = {'select', 'from', 'where', 'group', 'by', 'order', 
                       'having', 'and', 'or', 'not', 'in', 'as', 'sum', 
                       'count', 'avg', 'max', 'min', 'case', 'when', 'then', 
                       'else', 'end', 'null', 'nullif', 'coalesce'}
        
        for col in potential_columns:
            col_lower = col.lower()
            if (col_lower not in self.valid_columns and 
                col_lower not in sql_keywords and
                col_lower != self.table_name.lower() and
                not col_lower.endswith('_pct')):  # Allow calculated columns
                invalid.append(col)
        
        return list(set(invalid))
    
    def _check_aggregation(self, sql: str) -> List[str]:
        """Check for proper GROUP BY usage"""
        issues = []
        
        # Simple check - in production, use proper SQL parser
        if 'GROUP BY' in sql and 'SELECT' in sql:
            # Extract SELECT clause
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                
                # Check if non-aggregated columns are in GROUP BY
                if not any(agg in select_clause.upper() for agg in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
                    issues.append("SELECT contains columns without aggregation but has GROUP BY")
        
        return issues
    
    def _check_business_logic(self, sql_lower: str) -> List[str]:
        """Check for business logic issues"""
        warnings = []
        
        # Check for focus brands without filter
        if 'focus' in sql_lower and "status = 'focus'" not in sql_lower:
            warnings.append("Query mentions 'focus' but doesn't filter by Status = 'FOCUS'")
        
        # Check for achievement calculation
        if 'achievement' in sql_lower:
            if 'prim_value' not in sql_lower and 'sec_value' not in sql_lower:
                warnings.append("Achievement calculation should use Prim_Value or Sec_Value")
            if 'tgt_value' not in sql_lower and 'tgt_units' not in sql_lower:
                warnings.append("Achievement calculation should compare against target")
        
        # Check for YoY growth calculation
        if 'yoy' in sql_lower or 'year over year' in sql_lower:
            if 'ly_' not in sql_lower:
                warnings.append("YoY calculation should use LY_ (Last Year) columns")
        
        return warnings