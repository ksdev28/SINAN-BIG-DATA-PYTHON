
import duckdb
import pandas as pd
import sys
from pathlib import Path
import time

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.processors.sinan_data_processor_comprehensive import SINANDataProcessorComprehensive

def create_snapshot():
    print("=== Creating SINAN Snapshot Database ===")
    start_time = time.time()
    
    # Paths
    raw_data_path = project_root / "data/raw/VIOLBR-PARQUET"
    dict_path = project_root / "data/config/TAB_SINANONLINE"
    db_path = project_root / "data/processed/sinan.duckdb"
    
    # Ensure processed directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"[1] Initializing Processor...")
    processor = SINANDataProcessorComprehensive(raw_data_path, dict_path)
    
    # 1. Load Dictionaries
    processor.load_dictionaries()
    
    print(f"[2] Connecting to DuckDB...")
    # Connect to persistent DB
    conn = duckdb.connect(str(db_path))
    
    # 2. Load Raw Data (using DuckDB for speed)
    print(f"[3] Loading and Filtering Raw Data (0-17 years)...")
    
    # Register view
    parquet_pattern = str(raw_data_path / "*.parquet")
    conn.execute(f"CREATE OR REPLACE VIEW raw_view AS SELECT * FROM read_parquet('{parquet_pattern}', hive_partitioning=1, union_by_name=1)")
    
    # Filter by Age (0-17) immediately to reduce data size
    # Matches '4000' to '4017'
    age_codes = [f"40{i:02d}" for i in range(18)]
    age_list_str = "', '".join(age_codes)
    
    # Select distinct years first to process in batches if needed, 
    # but for 500k rows we can probably do it in memory or directly in SQL?
    # Let's try to pull all 0-17 data, decode in Python (since we have the complex dictionary logic there),
    # and then write back to DuckDB. 
    # This is "ETL" - Extract, Transform, Load.
    
    query = f"SELECT * FROM raw_view WHERE NU_IDADE_N IN ('{age_list_str}')"
    print(f"    Executing query...")
    df = conn.execute(query).df()
    print(f"    Loaded {len(df)} rows of raw child data.")
    
    # 3. Apply Dictionaries (Transform)
    print(f"[4] Applying Dictionaries (Python)...")
    t0 = time.time()
    df = processor.apply_dictionaries(df)
    
    # Apply Comprehensive Violence Filter (redundant but safe)
    # df = processor.filter_comprehensive_violence(df, already_filtered_by_age=True)
    # The filter_comprehensive_violence mostly filters by AGE (done) and 
    # checks at least one violence type is 'Sim'.
    # We might want to keep *all* 0-17 records even if no violence flagged? 
    # The dashboard seems to imply "Notificações de Violência", so yes, we should filter.
    
    df = processor.filter_comprehensive_violence(df, already_filtered_by_age=True)
    
    # Apply Derived Columns (ETL Logic moved from dashboard)
    # This prepares the data fully for the dashboard, removing runtime processing overhead.
    df = processor.create_derived_columns(df)
    
    print(f"    Transformations done in {time.time() - t0:.2f}s")
    print(f"    Final row count: {len(df)}")
    
    # 4. Save to Persistent DuckDB
    print(f"[5] Saving to {db_path}...")
    # Create table
    conn.execute("CREATE OR REPLACE TABLE violence_processed AS SELECT * FROM df")
    
    # Create indices for performance
    print(f"[6] Creating Indices...")
    # Index on commonly filtered columns
    if 'NU_ANO' in df.columns:
        # Create a clean integer year column for indexing if needed, 
        # but let's just index what we have.
        # Actually, let's CREATE a clean YEAR column here to avoid runtime casting!
        pass 
        
    # We should add the Derived Columns here too? 
    # Ideally YES. But `create_derived_columns` is in the dashboard file.
    # It would be better to keep the DB clean with "decoded" data and let dashboard do the last mile derivation 
    # OR move derivation here. 
    # Derivation includes "Faixa Etaria", "Tipo Violencia" (aggregated).
    # If we do it here, dashboard becomes super dump (FAST).
    # BUT `create_derived_columns` is complex and intertwined with Streamlit code structure.
    # Let's stick to: Decoded Data in DB.
    
    # Create indices
    conn.execute("CREATE INDEX IF NOT EXISTS idx_age ON violence_processed(NU_IDADE_N)")
    if 'DT_NOTIFIC' in df.columns:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON violence_processed(DT_NOTIFIC)")
    
    conn.close()
    
    print(f"=== Snapshot Created Successfully in {time.time() - start_time:.2f}s ===")

if __name__ == "__main__":
    create_snapshot()
