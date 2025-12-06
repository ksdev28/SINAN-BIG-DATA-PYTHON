
import duckdb
import pandas as pd
from pathlib import Path
import streamlit as st
from .sinan_data_processor_comprehensive import SINANDataProcessorComprehensive

        self.processor = SINANDataProcessorComprehensive(violence_data_path, dict_path)
        if not self.using_snapshot:
             self.processor.load_dictionaries()
        
    def get_available_years(self):
        """Returns sorted list of available years from data."""
        try:
            # Check if NU_ANO column exists
            columns = self._get_columns()
            if 'NU_ANO' in columns:
                # Use TRY_CAST to handle empty strings or non-numeric values safely
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
                # If returning codes, the dashboard might need to map them. 
                # But typically the dashboard expects names. 
                # For now, let's return what we have. 
                return df[col].tolist()
            return []
        except Exception as e:
            print(f"[DuckDB] Error getting UFs: {e}")
            return []

    def _get_columns(self):
        """Helper to get column names from the view."""
        return [col[0] for col in self.conn.execute(f"DESCRIBE {self.table_name}").fetchall()]

    def get_filtered_data(self, year_range=None, uf=None, municipio=None, violence_type=None):
        """
        Queries data based on filters and returns a Pandas DataFrame.
        """
        # OPTIMIZATION: Select only necessary columns to reduce memory usage and transfer time
        # These columns cover all charts and logic in the dashboard
        cols_to_select = [
            'DT_NOTIFIC', 'DT_OCOR', 'NU_ANO',  # Dates/Raw
            'ANO_NOTIFIC', 'UF_NOTIFIC', 'MUNICIPIO_NOTIFIC',  # Location/Time
            'TIPO_VIOLENCIA', 'FAIXA_ETARIA', 'SEXO',  # Demographics/Types
            'AUTOR_SEXO_CORRIGIDO', 'GRAU_PARENTESCO',  # Aggressor
            'TEMPO_OCOR_DENUNCIA', 'ENCAMINHAMENTOS_JUSTICA'  # KPIs
        ]
        
        # Verify columns exist before selecting (snapshot might vary) check vs self._get_columns() logic?
        # Ideally we just try select. But if we fall back to raw view, these cols wont exist.
        # Check self.using_snapshot
        
        if self.using_snapshot:
            cols_str = ", ".join(cols_to_select)
             # Add * if we suspect missing columns, but for speed we want specific.
             # Safe fallback: check one known derived column, if present assume all present.
            query_parts = [f"SELECT {cols_str} FROM {self.table_name} WHERE 1=1"]
        else:
            # Fallback for raw view (slower, selects all)
            query_parts = [f"SELECT * FROM {self.table_name} WHERE 1=1"]

        params = []
        
        # 1. Filter by Age (Only if NOT using snapshot, snapshot is already filtered)
        if not self.using_snapshot:
            age_codes = [f"40{i:02d}" for i in range(18)] 
            age_list_str = "', '".join(age_codes)
            query_parts.append(f"AND NU_IDADE_N IN ('{age_list_str}')")

        # 2. Filter by Year
        if year_range:
            start_year, end_year = year_range
            columns = self._get_columns()
            
            # Prioritize DT_NOTIFIC for year extraction matching Pandas/Snapshot logic
            if 'DT_NOTIFIC' in columns:
                 # If using snapshot, dates might already be correct types? 
                 # DuckDB preserves types. If they were strings in Parquet, they are strings here unless we casted.
                 # In create_snapshot we did NOT cast.
                 query_parts.append(f"AND CAST(SUBSTR(CAST(DT_NOTIFIC AS VARCHAR), 1, 4) AS INT) BETWEEN {start_year} AND {end_year}")
            elif 'NU_ANO' in columns:
                 query_parts.append(f"AND TRY_CAST(NU_ANO AS INT) BETWEEN {start_year} AND {end_year}")

        # 3. Filter by UF
        if uf and uf != 'Todos':
             # Map UF Name to Code (IBGE)
             uf_map = {
                'Rondônia': '11', 'Acre': '12', 'Amazonas': '13', 'Roraima': '14',
                'Pará': '15', 'Amapá': '16', 'Tocantins': '17', 'Maranhão': '21',
                'Piauí': '22', 'Ceará': '23', 'Rio Grande do Norte': '24', 'Paraíba': '25',
                'Pernambuco': '26', 'Alagoas': '27', 'Sergipe': '28', 'Bahia': '29',
                'Minas Gerais': '31', 'Espírito Santo': '32', 'Rio de Janeiro': '33',
                'São Paulo': '35', 'Paraná': '41', 'Santa Catarina': '42',
                'Rio Grande do Sul': '43', 'Mato Grosso do Sul': '50',
                'Mato Grosso': '51', 'Goiás': '52', 'Distrito Federal': '53'
             }
             
             uf_code = uf_map.get(uf)
             
             if uf_code:
                 columns = self._get_columns()
                 # Try common UF column names
                 if 'SG_UF_NOT' in columns:
                     query_parts.append(f"AND CAST(SG_UF_NOT AS VARCHAR) = '{uf_code}'")
                 elif 'SG_UF' in columns:
                     query_parts.append(f"AND CAST(SG_UF AS VARCHAR) = '{uf_code}'")

        full_query = " ".join(query_parts)
        print(f"[DuckDB] Executing: {full_query}")
        
        try:
            # OPTIMIZATION: Use Arrow for zero-copy transfer to Pandas
            # Handle both Table and RecordBatchReader (depending on versions)
            arrow_result = self.conn.execute(full_query).arrow()
            if hasattr(arrow_result, 'to_pandas'):
                df = arrow_result.to_pandas()
            else:
                # It's a RecordBatchReader
                df = arrow_result.read_all().to_pandas()
                
            print(f"[DuckDB] Fetched {len(df)} rows.")
            
            if not self.using_snapshot:
                # Apply Dictionaries (Decoding) ONLY if not using snapshot
                df = self.processor.apply_dictionaries(df)
                df = self.processor.filter_comprehensive_violence(df, already_filtered_by_age=True)
            
            return df
            
        except Exception as e:
            print(f"[DuckDB] Error executing query: {e}")
            return pd.DataFrame()
            
