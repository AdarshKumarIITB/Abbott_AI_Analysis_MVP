import yaml
from typing import Dict, List, Any
from pathlib import Path

class AbbottSchemaAdapter:
    """Adapter to convert analyzer.yaml schema to LangChain-compatible format"""
    
    def __init__(self, yaml_path: str):
        """Initialize with path to analyzer.yaml"""
        self.yaml_path = Path(yaml_path)
        
        # Load and parse the YAML file
        with open(self.yaml_path, 'r') as f:
            self.schema = yaml.safe_load(f)
    
    def get_custom_table_info(self) -> Dict[str, str]:
        """
        Convert analyzer.yaml to LangChain's custom_table_info format.
        Returns a dict with table_name as key and DDL-like string as value.
        """
        table_name = self.schema.get('table', 'analyzer')
        columns = []
        
        # Process each column definition
        for col in self.schema.get('columns', []):
            # Basic column definition
            col_def = f"{col['name']} {col['type']}"
            
            # Add nullability
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            
            # Add business context as SQL comments
            comments = []
            if col.get('business_name'):
                comments.append(f"Business Name: {col['business_name']}")
            if col.get('aliases'):
                aliases = ', '.join(col['aliases']) if isinstance(col['aliases'], list) else col['aliases']
                comments.append(f"Aliases: {aliases}")
            if col.get('business_description'):
                comments.append(col['business_description'])
            if col.get('example'):
                comments.append(f"Example: {col['example']}")
            if col.get('examples'):
                examples = ', '.join(str(e) for e in col['examples'])
                comments.append(f"Examples: {examples}")
            if col.get('special_considerations'):
                comments.append(f"Note: {col['special_considerations']}")
            
            if comments:
                col_def += f" -- {' | '.join(comments)}"
            
            columns.append(col_def)
        
        # Build the complete table info
        table_info = f"""CREATE TABLE {table_name} (
    {chr(10).join('    ' + col for col in columns)}
);

/* Table Description:
{self.schema.get('description', '')}

Notes:
{self.schema.get('notes', '')}
*/

/* Hierarchies: */
{self._format_hierarchies()}

/* Business Context and Rules: */
{self._format_business_context()}

/* Available Metrics: */
{self._format_metrics()}
"""
        
        return {table_name: table_info}
    
    def _format_hierarchies(self) -> str:
        """Format hierarchy information for inclusion in table info"""
        hierarchies = self.schema.get('hierarchies', {})
        if not hierarchies:
            return "No hierarchies defined"
        
        formatted = []
        for name, hierarchy in hierarchies.items():
            formatted.append(f"\n{hierarchy.get('name', name)} Hierarchy:")
            
            # List levels
            if 'levels' in hierarchy:
                for level in hierarchy['levels']:
                    level_cols = ', '.join(level.get('columns', []))
                    formatted.append(f"  - {level.get('name', 'Unnamed')}: {level_cols}")
            
            # List relationships
            if 'relationships' in hierarchy:
                formatted.append("  Relationships:")
                for rel in hierarchy['relationships']:
                    formatted.append(f"    - {rel.get('parent', '')} -> {rel.get('child', '')}")
        
        return '\n'.join(formatted)
    
    def _format_business_context(self) -> str:
        """Format business context for inclusion in table info"""
        context = self.schema.get('business_context', {})
        if not context:
            return "No business context defined"
        
        formatted = []
        
        # Vocabulary
        if 'vocabulary' in context:
            formatted.append("\nBusiness Vocabulary:")
            for category, terms in context['vocabulary'].items():
                formatted.append(f"  {category}:")
                for term, definition in terms.items():
                    if isinstance(definition, dict):
                        formatted.append(f"    - '{term}': {definition}")
                    else:
                        formatted.append(f"    - '{term}': {definition}")
        
        # Performance terms
        if 'performance_terms' in context:
            formatted.append("\nPerformance Terms:")
            for term, definition in context['performance_terms'].items():
                formatted.append(f"  - '{term}': {definition}")
        
        # Rules
        if 'rules' in context:
            formatted.append("\nBusiness Rules:")
            for rule_category, rules in context['rules'].items():
                formatted.append(f"  {rule_category}:")
                for rule in rules:
                    formatted.append(f"    - {rule.get('rule', '')}: {rule.get('description', '')}")
        
        # Special columns
        if 'special_columns' in context:
            formatted.append("\nSpecial Column Handling:")
            for col, info in context['special_columns'].items():
                formatted.append(f"  - {col}: {info}")
        
        return '\n'.join(formatted)
    
    def _format_metrics(self) -> str:
        """Format metrics definitions for inclusion in table info"""
        metrics = self.schema.get('metrics', {})
        if not metrics:
            return "No metrics defined"
        
        formatted = []
        for metric_key, metric_def in metrics.items():
            name = metric_def.get('name', metric_key)
            formula = metric_def.get('formula', 'No formula')
            description = metric_def.get('description', '')
            
            formatted.append(f"\n{metric_key}:")
            formatted.append(f"  Name: {name}")
            formatted.append(f"  Formula: {formula}")
            if description:
                formatted.append(f"  Description: {description}")
            if 'base_columns' in metric_def:
                formatted.append(f"  Required columns: {', '.join(metric_def['base_columns'])}")
        
        return '\n'.join(formatted)
    
    def get_business_context(self) -> Dict[str, Any]:
        """Extract business vocabulary and rules as a dictionary"""
        return self.schema.get('business_context', {})
    
    def get_metrics_definitions(self) -> Dict[str, Any]:
        """Extract calculated metrics as a dictionary"""
        return self.schema.get('metrics', {})
    
    def get_hierarchies(self) -> Dict[str, Any]:
        """Extract hierarchies as a dictionary"""
        return self.schema.get('hierarchies', {})
    
    def get_table_name(self) -> str:
        """Get the table name from schema"""
        return self.schema.get('table', 'analyzer')