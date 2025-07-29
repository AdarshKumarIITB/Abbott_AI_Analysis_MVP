from typing import List, Dict

class AbbottFewShotExamples:
    """Few-shot examples for Abbott sales analytics queries"""
    
    @staticmethod
    def get_examples() -> List[Dict[str, str]]:
        """Return a list of question-SQL pairs for few-shot learning"""
        return [
            {
                "question": "Which territories did not achieve target in North zone?",
                "query": """SELECT Terr_Code, TBM_Name, 
                       (Prim_Value / NULLIF(Tgt_Value, 0)) * 100 as achievement_pct
                FROM analyzer
                WHERE Zone = 'North' 
                AND Mth != 'All'
                AND Tgt_Value > 0
                AND Prim_Value < Tgt_Value
                ORDER BY achievement_pct ASC;"""
            },
            {
                "question": "Show YoY growth for focus brands",
                "query": """SELECT Brand, 
                       SUM(Prim_Value) as current_sales,
                       SUM(LY_Prim_Value) as last_year_sales,
                       ((SUM(Prim_Value) - SUM(LY_Prim_Value)) / 
                        NULLIF(SUM(LY_Prim_Value), 0)) * 100 as yoy_growth_pct
                FROM analyzer
                WHERE Status = 'FOCUS'
                AND Mth != 'All'
                GROUP BY Brand
                ORDER BY yoy_growth_pct DESC;"""
            },
            {
                "question": "Top 5 underperforming territories",
                "query": """SELECT Terr_Code, TBM_Name, Zone,
                       SUM(Prim_Value) as actual_sales,
                       SUM(Tgt_Value) as target_sales,
                       (SUM(Prim_Value) / NULLIF(SUM(Tgt_Value), 0)) * 100 as achievement_pct
                FROM analyzer
                WHERE Mth != 'All'
                AND Tgt_Value > 0
                GROUP BY Terr_Code, TBM_Name, Zone
                HAVING (SUM(Prim_Value) / NULLIF(SUM(Tgt_Value), 0)) < 1
                ORDER BY achievement_pct ASC
                LIMIT 5;"""
            }
        ]
    
    @staticmethod
    def get_validation_examples() -> List[Dict[str, str]]:
        """Examples of common SQL mistakes and corrections"""
        return [
            {
                "mistake": "Not excluding Mth = 'All'",
                "correct": "Always add WHERE Mth != 'All' unless totals are specifically requested"
            },
            {
                "mistake": "Division by zero in calculations",
                "correct": "Use NULLIF(denominator, 0) to avoid division by zero"
            },
            {
                "mistake": "Not handling NULL values",
                "correct": "Use COALESCE or NULLIF for safe calculations"
            }
        ]