
import sys
import time
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.processors.sinan_duckdb_adapter import SINANDuckDBAdapter

def verify():
    print("=== Verification of DuckDB Adapter ===")
    
    start_time = time.time()
    try:
        print("[1] Initializing Adapter...")
        adapter = SINANDuckDBAdapter(
            violence_data_path=str(project_root / "data/raw/VIOLBR-PARQUET"),
            dict_path=str(project_root / "data/config/TAB_SINANONLINE")
        )
        print(f"    Initialized in {time.time() - start_time:.4f}s")
        
        print("[2] Checking Available Years...")
        t0 = time.time()
        years = adapter.get_available_years()
        print(f"    Years found: {years}")
        print(f"    Time: {time.time() - t0:.4f}s")
        
        if not years:
            print("    [ERROR] No years found!")
        
        print("[3] Checking Available UFs...")
        t0 = time.time()
        ufs = adapter.get_available_ufs()
        print(f"    UFs found: {len(ufs)} UFs")
        print(f"    Time: {time.time() - t0:.4f}s")
        
        print("[4] Fetching Filtered Data (Year=2023)...")
        t0 = time.time()
        df = adapter.get_filtered_data(year_range=[2023, 2023])
        if df is None or df.empty:
            print("    [WARNING] No data found for 2023. Trying all years...")
            df = adapter.get_filtered_data()
            
        print(f"    Fetched {len(df)} rows.")
        print(f"    Columns: {len(df.columns)}")
        print(f"    Time: {time.time() - t0:.4f}s")
        
        if 'NU_IDADE_N' in df.columns:
            print("    [OK] NU_IDADE_N present")
        else:
             print("    [ERROR] NU_IDADE_N missing")
             
        # Check decoding
        if 'CS_SEXO' in df.columns:
             sample = df['CS_SEXO'].unique()
             print(f"    Sex values (decoded?): {sample}")
             
        print("=== Verification Complete ===")
        
    except Exception as e:
        print(f"[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify()
