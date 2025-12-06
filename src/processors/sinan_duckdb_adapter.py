
import duckdb
import pandas as pd
from pathlib import Path
import streamlit as st
import os
from .sinan_data_processor_comprehensive import SINANDataProcessorComprehensive

class SINANDuckDBAdapter:
    """
    Adapter to interact with SINAN data using DuckDB for efficient on-demand querying.
    Replaces loading the entire dataset into memory.
    """
    
    def __init__(self, db_path="data/processed/sinan.duckdb"):
        # Calculate root carefully regardless of where this is run
        # __file__ = src/processors/sinan_duckdb_adapter.py
        # parent = src/processors
        # parent.parent = src
        # parent.parent.parent = ROOT
        self.project_root = Path(__file__).parent.parent.parent
        self.db_path = self.project_root / db_path
        
        # Paths for fallback (if needed, though Parquet might not be there on deploy)
        self.violence_data_path = self.project_root / "data/raw/VIOLBR-PARQUET"
        self.dict_path = self.project_root / "data/config/TAB_SINANONLINE"
        
        self.conn = None
        self.processor = SINANDataProcessorComprehensive(
            str(self.violence_data_path), 
            str(self.dict_path)
        )
        self.using_snapshot = False

        # Verifica se o banco existe ou precisa ser remontado (Split Strategy)
        self._check_and_reassemble_db()

        if self.db_path.exists():
            try:
                print(f"[DuckDB] Connecting to snapshot database: {self.db_path}")
                # read_only=True allows concurrent access which is good for web apps
                self.conn = duckdb.connect(str(self.db_path), read_only=True)
                
                # Test connection and set table name
                self.conn.execute("SELECT 1")
                self.table_name = "violence_processed"
                self.using_snapshot = True
                print("[DuckDB] Connected successfully.")
            except Exception as e:
                print(f"[DuckDB] Snapshot exists but failed to load: {e}. Falling back to raw.")
                self.using_snapshot = False
        
        if not self.using_snapshot:
            print("[DuckDB] Snapshot not found or invalid. Using raw views.")
            self.conn = duckdb.connect(":memory:")
            # If raw parquet files exist (local dev), use them
            if self.violence_data_path.exists() and list(self.violence_data_path.glob("*.parquet")):
                parquet_pattern = str(self.violence_data_path / "*.parquet")
                self.conn.execute(f"CREATE OR REPLACE VIEW violence_raw AS SELECT * FROM read_parquet('{parquet_pattern}', hive_partitioning=1, union_by_name=1)")
                self.table_name = "violence_raw"
                # Initialize processor dictionaries for raw data mode
                self.processor.load_dictionaries()
            else:
                 print("[DuckDB] CRITICAL: No snapshot and no raw parquet files found.")
                 # Create empty table to prevent crash, but app will be empty
                 self.conn.execute("CREATE TABLE violence_raw (dummy int)")
                 self.table_name = "violence_raw"

    def _check_and_reassemble_db(self):
        """Remonta o banco de dados se existirem partes divididas (deploy fix)."""
        # Se o arquivo não existe ou é muito pequeno (ex: pointer LFS inválido), tenta remontar
        if not self.db_path.exists() or self.db_path.stat().st_size < 2000:
            # Procurar partes na mesma pasta do db (data/processed)
            parent_dir = self.db_path.parent
            parts = sorted([p for p in parent_dir.glob("sinan.duckdb.part*")])
            
            if parts:
                print(f"[DuckDB] Reassembling database from {len(parts)} parts...")
                try:
                    with open(self.db_path, 'wb') as outfile:
                        for part in parts:
                            with open(part, 'rb') as infile:
                                outfile.write(infile.read())
                    print("[DuckDB] Database reassembled successfully.")
                except Exception as e:
                    print(f"[DuckDB] Failed to reassemble database: {e}")
            else:
                print("[DuckDB] No split parts found to reassemble.")

    def get_available_years(self):
        """Returns sorted list of available years from data."""
        try:
            # Check if NU_ANO column exists
            columns = self._get_columns()
            if 'NU_ANO' in columns:
                query = f"SELECT DISTINCT TRY_CAST(NU_ANO AS INT) as ano FROM {self.table_name} WHERE TRY_CAST(NU_ANO AS INT) IS NOT NULL ORDER BY 1"
                df = self.conn.execute(query).df()
                return sorted(df['ano'].tolist())
            elif 'DT_NOTIFIC' in columns:
                 # Extract year from date string YYYYMMDD
                 query = f"SELECT DISTINCT CAST(SUBSTR(CAST(DT_NOTIFIC AS VARCHAR), 1, 4) AS INT) as ano FROM {self.table_name} WHERE DT_NOTIFIC IS NOT NULL ORDER BY 1"
                 df = self.conn.execute(query).df()
                 return sorted(df['ano'].tolist())
            return []
        except Exception as e:
            print(f"[DuckDB] Error getting years: {e}")
            return []

    def get_available_ufs(self):
        """Returns sorted list of UFs (States) present in data."""
        try:
            columns = self._get_columns()
            # Prioritize the derived name column if it exists (Snapshot)
            if 'UF_NOTIFIC' in columns:
                query = f"SELECT DISTINCT UF_NOTIFIC FROM {self.table_name} WHERE UF_NOTIFIC IS NOT NULL AND UF_NOTIFIC != 'N/A' AND UF_NOTIFIC != 'Não informado' ORDER BY 1"
                df = self.conn.execute(query).df()
                return df['UF_NOTIFIC'].tolist()
            
            # Fallback to codes (Raw)
            col = 'SG_UF_NOT' if 'SG_UF_NOT' in columns else 'SG_UF' if 'SG_UF' in columns else None
            
            if col:
                query = f"SELECT DISTINCT {col} FROM {self.table_name} WHERE {col} IS NOT NULL ORDER BY 1"
                df = self.conn.execute(query).df()
                return df[col].tolist()
            return []
        except Exception as e:
            print(f"[DuckDB] Error getting UFs: {e}")
            return []

    def _get_columns(self):
        """Helper to get column names from the view."""
        try:
             return [col[0] for col in self.conn.execute(f"DESCRIBE {self.table_name}").fetchall()]
        except:
             return []

    def get_filtered_data(self, year_range=None, uf=None, municipio=None, violence_type=None):
        """
        Queries data based on filters and returns a Pandas DataFrame.
        """
        # OPTIMIZATION: Select only necessary columns
        cols_to_select = [
            'DT_NOTIFIC', 'DT_OCOR', 'NU_ANO', 
            'ANO_NOTIFIC', 'UF_NOTIFIC', 'MUNICIPIO_NOTIFIC', 
            'TIPO_VIOLENCIA', 'FAIXA_ETARIA', 'SEXO', 
            'AUTOR_SEXO_CORRIGIDO', 'GRAU_PARENTESCO', 
            'TEMPO_OCOR_DENUNCIA', 'ENCAMINHAMENTOS_JUSTICA'
        ]
        
        if self.using_snapshot:
            cols_str = ", ".join(cols_to_select)
            query_parts = [f"SELECT {cols_str} FROM {self.table_name} WHERE 1=1"]
        else:
            query_parts = [f"SELECT * FROM {self.table_name} WHERE 1=1"]

        # 1. Age Filter (If raw)
        if not self.using_snapshot:
            age_codes = [f"40{i:02d}" for i in range(18)] 
            age_list_str = "', '".join(age_codes)
            query_parts.append(f"AND NU_IDADE_N IN ('{age_list_str}')")

        # 2. Year Filter
        if year_range:
            start_year, end_year = year_range
            columns = self._get_columns()
            if 'DT_NOTIFIC' in columns:
                 query_parts.append(f"AND CAST(SUBSTR(CAST(DT_NOTIFIC AS VARCHAR), 1, 4) AS INT) BETWEEN {start_year} AND {end_year}")
            elif 'NU_ANO' in columns:
                 query_parts.append(f"AND TRY_CAST(NU_ANO AS INT) BETWEEN {start_year} AND {end_year}")

        # 3. UF Filter
        if uf and uf != 'Todos':
             # Simple map for raw data codes if needed, or rely on snapshot names
             uf_map = {'Rondônia': '11', 'Acre': '12', 'Amazonas': '13', 'Roraima': '14', 'Pará': '15', 'Amapá': '16', 'Tocantins': '17', 'Maranhão': '21', 'Piauí': '22', 'Ceará': '23', 'Rio Grande do Norte': '24', 'Paraíba': '25', 'Pernambuco': '26', 'Alagoas': '27', 'Sergipe': '28', 'Bahia': '29', 'Minas Gerais': '31', 'Espírito Santo': '32', 'Rio de Janeiro': '33', 'São Paulo': '35', 'Paraná': '41', 'Santa Catarina': '42', 'Rio Grande do Sul': '43', 'Mato Grosso do Sul': '50', 'Mato Grosso': '51', 'Goiás': '52', 'Distrito Federal': '53'}
             uf_code = uf_map.get(uf)
             
             columns = self._get_columns()
             if 'UF_NOTIFIC' in columns: # Snapshot has names
                  query_parts.append(f"AND UF_NOTIFIC = '{uf}'")
             elif uf_code and 'SG_UF_NOT' in columns: # Raw has codes
                  query_parts.append(f"AND CAST(SG_UF_NOT AS VARCHAR) = '{uf_code}'")

        full_query = " ".join(query_parts)
        print(f"[DuckDB] Executing: {full_query}")
        
        try:
            # Use Arrow for speed
            arrow_result = self.conn.execute(full_query).arrow()
            if hasattr(arrow_result, 'to_pandas'):
                df = arrow_result.to_pandas()
            else:
                df = arrow_result.read_all().to_pandas()
                
            print(f"[DuckDB] Fetched {len(df)} rows.")
            
            if not self.using_snapshot:
                # Apply dictionaries if raw
                if hasattr(self.processor, 'apply_dictionaries'):
                    df = self.processor.apply_dictionaries(df)
                    df = self.processor.filter_comprehensive_violence(df, already_filtered_by_age=True)
            
            return df
            
        except Exception as e:
            print(f"[DuckDB] Error executing query: {e}")
            return pd.DataFrame()
