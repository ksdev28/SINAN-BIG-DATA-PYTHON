#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Processador de Dados SINAN usando DuckDB para melhor performance
DuckDB permite consultas SQL diretas em arquivos parquet sem carregar tudo na memória
"""

import duckdb
import pandas as pd
from pathlib import Path
from sinan_data_processor_comprehensive import SINANDataProcessorComprehensive

class SINANDataProcessorDuckDB:
    """
    Processador otimizado usando DuckDB para consultas eficientes em grandes volumes
    """
    
    def __init__(self, violence_data_path="data/raw/VIOLBR-PARQUET", dict_path="data/config/TAB_SINANONLINE"):
        self.violence_data_path = Path(violence_data_path)
        self.dict_path = Path(dict_path)
        self.conn = duckdb.connect()
        self.processor = SINANDataProcessorComprehensive(violence_data_path, dict_path)
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
    
    def load_dictionaries(self):
        """Carrega dicionários usando o processador original"""
        return self.processor.load_dictionaries()
    
    def get_parquet_files(self):
        """Retorna lista de arquivos parquet"""
        parquet_files = list(self.violence_data_path.glob("*.parquet"))
        return [str(f) for f in parquet_files]
    
    def query_with_filters(self, filters=None, columns=None):
        """
        Executa consulta SQL nos arquivos parquet com filtros aplicados
        
        Args:
            filters: dict com filtros SQL (ex: {'NU_IDADE_N': "IN ('4000', '4001', ...)"})
            columns: lista de colunas a selecionar (None = todas)
        
        Returns:
            DataFrame com resultados
        """
        parquet_files = self.get_parquet_files()
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Construir query SQL
        if columns:
            cols_str = ', '.join(columns)
        else:
            cols_str = '*'
        
        # Criar UNION ALL de todos os arquivos parquet
        queries = []
        for file_path in parquet_files:
            query = f"SELECT {cols_str} FROM read_parquet('{file_path}')"
            
            # Adicionar filtros WHERE
            if filters:
                where_clauses = []
                for col, condition in filters.items():
                    where_clauses.append(f"{col} {condition}")
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
            
            queries.append(query)
        
        # Combinar todas as queries
        full_query = " UNION ALL ".join(queries)
        
        # Executar e retornar DataFrame
        result = self.conn.execute(full_query).df()
        return result
    
    def load_filtered_violence_data(self, age_filter=True, violence_filter=True):
        """
        Carrega dados filtrados usando DuckDB (muito mais eficiente)
        
        Args:
            age_filter: Se True, filtra apenas 0-17 anos
            violence_filter: Se True, filtra apenas casos de violência
        
        Returns:
            DataFrame filtrado
        """
        print("🚀 Carregando dados com DuckDB (otimizado)...")
        
        # Definir colunas essenciais
        essential_columns = [
            'DT_NOTIFIC', 'NU_ANO', 'SG_UF_NOT', 'SG_UF', 'ID_MUNICIP', 'ID_MN_RESI',
            'NU_IDADE_N', 'CS_SEXO', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_SEXU', 'VIOL_INFAN',
            'LOCAL_OCOR', 'AUTOR_SEXO', 'AUTOR_ALCO', 'CS_ESCOL_N', 'CS_RACA',
            'SIT_CONJUG', 'ID_OCUPA_N', 'REDE_SAU', 'REDE_EDUCA'
        ]
        
        # Colunas adicionais importantes para análises avançadas
        additional_columns = [
            'DT_OCOR',      # Data de ocorrência (para calcular tempo até denúncia)
            'DT_ENCERRA',   # Data de encerramento (para status do caso)
            'DT_DIGITA',    # Data de digitação
            'DT_INVEST',    # Data de investigação
            'EVOLUCAO',     # Evolução do caso (encerrado, abandonado, etc.)
            'ENC_DELEG',    # Encaminhamento para delegacia
            'ENC_DPCA',     # Encaminhamento para DPCA (Delegacia de Proteção à Criança)
            'ENC_MPU',      # Encaminhamento para Ministério Público
            'ENC_VARA',     # Encaminhamento para Vara da Infância
            'DELEG',        # Delegacia
            'DELEG_CRIA',   # Delegacia da Criança
            'DELEG_IDOS',   # Delegacia do Idoso
            'DELEG_MULH',   # Delegacia da Mulher
            'HORA_OCOR',    # Hora da ocorrência
            'CLASSI_FIN'    # Classificação final
        ]
        
        # Combinar todas as colunas
        essential_columns = essential_columns + additional_columns
        
        # Adicionar todas as colunas REL_
        # Primeiro, precisamos descobrir quais colunas existem
        # Vamos fazer uma query rápida para verificar
        sample_query = f"SELECT * FROM read_parquet('{self.get_parquet_files()[0]}') LIMIT 1"
        sample_df = self.conn.execute(sample_query).df()
        rel_columns = [col for col in sample_df.columns if col.startswith('REL_')]
        
        all_columns = essential_columns + rel_columns
        # Filtrar apenas colunas que existem
        available_columns = [col for col in all_columns if col in sample_df.columns]
        
        # Construir filtros
        filters = {}
        
        if age_filter:
            # Códigos de idade: 4000 (menor de 1 ano) até 4017 (17 anos)
            age_codes = ['4000'] + [f'400{i}' for i in range(1, 18)]
            age_list = "', '".join(age_codes)
            filters['NU_IDADE_N'] = f"IN ('{age_list}')"
        
        # Carregar dados filtrados
        print(f"   📊 Aplicando filtros: idade={age_filter}, violência={violence_filter}")
        df = self.query_with_filters(filters=filters, columns=available_columns)
        
        print(f"   ✅ Registros carregados: {len(df):,}")
        
        # Aplicar filtro de violência se necessário (usando pandas após carregar)
        if violence_filter and len(df) > 0:
            print("   🔍 Filtrando por tipo de violência...")
            violence_columns = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
            available_violence_cols = [col for col in violence_columns if col in df.columns]
            
            if available_violence_cols:
                # Criar condição: pelo menos uma coluna de violência = '1' ou 1
                violence_conditions = []
                for col in available_violence_cols:
                    col_str = df[col].astype(str)
                    col_condition = col_str.isin(['1', 'SIM', 'S', '1.0'])
                    if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                        col_num = (df[col] == 1)
                        col_condition = col_condition | col_num
                    violence_conditions.append(col_condition)
                
                combined_condition = violence_conditions[0]
                for condition in violence_conditions[1:]:
                    combined_condition = combined_condition | condition
                
                df_filtered = df[combined_condition].copy()
                
                print(f"   ✅ Casos de violência: {len(df_filtered):,}")
                return df_filtered
        
        return df
    
    def aggregate_by_filters(self, group_by_columns, filters=None):
        """
        Agrega dados diretamente no DuckDB (muito mais rápido que pandas)
        
        Args:
            group_by_columns: lista de colunas para GROUP BY
            filters: dict com filtros SQL
        
        Returns:
            DataFrame agregado
        """
        parquet_files = self.get_parquet_files()
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Construir query de agregação
        group_by_str = ', '.join(group_by_columns)
        
        queries = []
        for file_path in parquet_files:
            query = f"""
                SELECT {group_by_str}, COUNT(*) as contagem
                FROM read_parquet('{file_path}')
            """
            
            if filters:
                where_clauses = []
                for col, condition in filters.items():
                    where_clauses.append(f"{col} {condition}")
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
            
            query += f" GROUP BY {group_by_str}"
            queries.append(query)
        
        # Combinar e agregar novamente (para UNION ALL)
        full_query = f"""
            SELECT {group_by_str}, SUM(contagem) as contagem
            FROM (
                {' UNION ALL '.join(queries)}
            )
            GROUP BY {group_by_str}
        """
        
        result = self.conn.execute(full_query).df()
        return result

