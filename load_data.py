import pandas as pd
import duckdb
from pathlib import Path

def load_excel_to_duckdb():
    """Load Analyzer.xlsx into DuckDB database"""
    
    # Paths
    excel_path = Path("data/Analyzer.xlsx")
    db_path = Path("local.duckdb")
    
    if not excel_path.exists():
        print(f"Error: {excel_path} not found")
        return
    
    # Read Excel file
    print("Reading Excel file...")
    try:
        df = pd.read_excel(excel_path)
        print(f"Loaded {len(df)} rows and {len(df.columns)} columns from Excel")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # Clean column names if needed
    df.columns = df.columns.str.strip()
    
    # Show column info
    print("\nColumns found in Excel:")
    for i, col in enumerate(df.columns):
        print(f"  {i+1}. {col}")
    
    # Connect to DuckDB
    print(f"\nCreating DuckDB database at {db_path}...")
    try:
        conn = duckdb.connect(str(db_path))
        
        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS analyzer")
        
        # Create table from DataFrame
        print("Loading data into analyzer table...")
        conn.execute("CREATE TABLE analyzer AS SELECT * FROM df")
        
        # Verify data
        result = conn.execute("SELECT COUNT(*) FROM analyzer").fetchone()
        print(f"Successfully loaded {result[0]} rows into analyzer table")
        
        # Show sample data
        print("\nSample data from analyzer table:")
        sample = conn.execute("SELECT * FROM analyzer LIMIT 3").fetchall()
        columns = [desc[0] for desc in conn.description]
        
        print(f"Columns: {columns[:5]}...")  # Show first 5 columns
        for i, row in enumerate(sample):
            print(f"Row {i+1}: {row[:5]}...")  # Show first 5 values
        
        # Show table schema
        print("\nTable schema:")
        schema = conn.execute("DESCRIBE analyzer").fetchall()
        for col_name, col_type, null, key, default, extra in schema[:10]:  # First 10 columns
            print(f"  {col_name}: {col_type}")
        
        conn.close()
        print(f"\nDatabase created successfully: {db_path}")
        
    except Exception as e:
        print(f"Error creating database: {e}")
        return

def verify_database():
    """Verify the database has been created correctly"""
    db_path = Path("local.duckdb")
    
    if not db_path.exists():
        print("Database file does not exist. Please run load_excel_to_duckdb() first.")
        return
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # Check if analyzer table exists
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"Tables in database: {[t[0] for t in tables]}")
        
        if ('analyzer',) in tables:
            # Get row count
            count = conn.execute("SELECT COUNT(*) FROM analyzer").fetchone()[0]
            print(f"Analyzer table has {count} rows")
            
            # Check for required columns
            columns = conn.execute("DESCRIBE analyzer").fetchall()
            col_names = [col[0] for col in columns]
            
            required_cols = ['Mth', 'Brand', 'Zone', 'Prim_Value', 'Tgt_Value']
            missing_cols = [col for col in required_cols if col not in col_names]
            
            if missing_cols:
                print(f"Warning: Missing expected columns: {missing_cols}")
            else:
                print("All key columns found!")
                
            # Test a simple query
            test_result = conn.execute("""
                SELECT Zone, COUNT(*) as records 
                FROM analyzer 
                WHERE Mth != 'All' 
                GROUP BY Zone 
                LIMIT 5
            """).fetchall()
            
            print(f"Sample query result: {test_result}")
            
        conn.close()
        
    except Exception as e:
        print(f"Error verifying database: {e}")

if __name__ == "__main__":
    print("=== Abbott Data Loader ===")
    load_excel_to_duckdb()
    print("\n=== Database Verification ===")
    verify_database()