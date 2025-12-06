#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Processador Completo de Dados SINAN
Inclui todos os tipos de violência e análises socioeconômicas
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import time

class SINANDataProcessorComprehensive:
    """
    Processador completo de dados SINAN com análises socioeconômicas
    """
    
    def __init__(self, violence_data_path="data/raw/VIOLBR-PARQUET", dict_path="data/config/TAB_SINANONLINE", population_data_path="data/raw/POPS-PARQUET"):
        self.violence_data_path = Path(violence_data_path)
        self.dict_path = Path(dict_path)
        self.population_data_path = Path(population_data_path)
        self.dictionaries = {}
        self.violence_data = None
        self.population_data = None
        self.processed_data = None
        
    def load_dictionaries(self):
        """
        Carrega dicionários básicos para decodificação
        """
        print("[INFO] Carregando dicionarios basicos...")
        
        # Dicionários básicos para decodificação
        self.dictionaries = {
            'VIOL_SEXU': {'1': 'Sim', '2': 'No', '9': 'Ignorado', '': 'Branco'},
            'VIOL_FISIC': {'1': 'Sim', '2': 'No', '9': 'Ignorado', '': 'Branco'},
            'VIOL_PSICO': {'1': 'Sim', '2': 'No', '9': 'Ignorado', '': 'Branco'},
            'VIOL_INFAN': {'1': 'Sim', '2': 'No', '9': 'Ignorado', '': 'Branco'},
            'CS_SEXO': {'1': 'Masculino', '2': 'Feminino', '9': 'Ignorado', '': 'Branco'},
            'AUTOR_SEXO': {'1': 'Masculino', '2': 'Feminino', '9': 'Ignorado', '3': 'Outros', '': 'Branco'},
            'AUTOR_ALCO': {'1': 'Sim', '2': 'No', '9': 'Ignorado', '3': 'Não se aplica', '': 'Branco'},
            'LOCAL_OCOR': {
                '01': 'Residência', '02': 'Habitação coletiva', '03': 'Escola', '04': 'Local de prática esportiva',
                '05': 'Bar ou similar', '06': 'Via pública', '07': 'Comércio e serviços', '08': 'Industrias e construção',
                '09': 'Outros', '99': 'Ignorado'
            },
            'CS_ESCOL_N': {
                '00': 'Analfabeto', '01': '1ª a 4ª série incompleta do EF', '02': '4ª série completa do EF',
                '03': '5ª à 8ª série incompleta do EF', '04': 'Ensino fundamental completo',
                '05': 'Ensino médio incompleto', '06': 'Ensino médio completo',
                '07': 'Educação superior incompleta', '08': 'Educação superior completa',
                '09': 'Ignorado', '10': 'Não se aplica'
            },
            'CS_RACA': {
                '1': 'Branca', '2': 'Preta', '3': 'Amarela', '4': 'Parda', '5': 'Indígena', '9': 'Ignorado'
            },
            'SIT_CONJUG': {
                '1': 'Solteiro', '2': 'Casado', '3': 'Viúvo', '4': 'Separado', '8': 'Não se aplica', '9': 'Ignorado'
            },
            'REL_TRAB': {'1': 'Sim', '2': 'Não', '9': 'Ignorado', '': 'Branco'},
            'REL_CAT': {'1': 'Empregado', '2': 'Autônomo', '8': 'Não se aplica', '9': 'Ignorado', '': 'Branco'},
            'REDE_SAU': {'1': 'Sim', '2': 'Não', '9': 'Ignorado', '': 'Branco'},
            'REDE_EDUCA': {'1': 'Sim', '2': 'Não', '9': 'Ignorado', '': 'Branco'}
        }
        
        # Dicionário de idade
        age_mapping = {}
        for i in range(1, 18):
            if i == 1:
                age_mapping['4001'] = '01 ano'
            else:
                age_mapping[f'40{i:02d}'] = f'{i:02d} anos'
        age_mapping['4000'] = 'menor de 01 ano'
        self.dictionaries['NU_IDADE_N'] = age_mapping
        
        print(f"[OK] {len(self.dictionaries)} dicionarios carregados")
        
    def load_violence_data(self):
        """
        Carrega dados de violência de todos os arquivos Parquet
        """
        print("[INFO] Carregando dados de violencia...")
        
        parquet_files = list(self.violence_data_path.glob("*.parquet"))
        print(f"   [INFO] Encontrados {len(parquet_files)} arquivos")
        
        all_data = []
        
        for file_path in parquet_files:
            try:
                df = pd.read_parquet(file_path)
                all_data.append(df)
                print(f"   [OK] {file_path.name}: {len(df):,} registros")
            except Exception as e:
                print(f"   [ERRO] Erro ao carregar {file_path.name}: {e}")
        
        if all_data:
            self.violence_data = pd.concat(all_data, ignore_index=True)
            print(f"[OK] Total de registros carregados: {len(self.violence_data):,}")
        else:
            print("[ERRO] Nenhum dado carregado")
            
        return self.violence_data
    
    def apply_dictionaries(self, data, columns_to_decode=None):
        """
        Aplica todos os dicionários aos dados (otimizado para grandes volumes)
        """
        print("[INFO] Aplicando dicionarios...")
        
        # Se não especificar colunas, usar todas as disponíveis
        if columns_to_decode is None:
            columns_to_decode = data.columns.tolist()
        
        # Processar apenas as colunas necessárias, evitando cópia completa
        # Criar novo DataFrame apenas com as colunas que serão modificadas
        decoded_data = data.copy(deep=False)  # Shallow copy para economizar memória
        
        # Mapear colunas para dicionários
        column_mappings = {
            'CS_ESCOL_N': 'CS_ESCOL_N',
            'CS_RACA': 'CS_RACA', 
            'ID_OCUPA_N': 'ID_OCUPA_N',
            'SIT_CONJUG': 'SIT_CONJUG',
            'VIOL_SEXU': 'VIOL_SEXU',
            'VIOL_FISIC': 'VIOL_FISIC',
            'VIOL_PSICO': 'VIOL_PSICO',
            'VIOL_INFAN': 'VIOL_INFAN',
            'NU_IDADE_N': 'NU_IDADE_N',
            'CS_SEXO': 'CS_SEXO',
            'LOCAL_OCOR': 'LOCAL_OCOR',
            'AUTOR_SEXO': 'AUTOR_SEXO',
            'AUTOR_ALCO': 'AUTOR_ALCO',
            'REL_TRAB': 'REL_TRAB',
            'REL_CAT': 'REL_CAT',
            'REDE_SAU': 'REDE_SAU',
            'REDE_EDUCA': 'REDE_EDUCA'
        }
        
        # Aplicar dicionários apenas nas colunas que existem e precisam ser decodificadas
        for column, dict_name in column_mappings.items():
            if column in decoded_data.columns and dict_name in self.dictionaries:
                print(f"   [INFO] Aplicando {dict_name} em {column}")
                # Usar map com fillna para manter valores não mapeados
                dict_map = self.dictionaries[dict_name]
                # Converter para string e aplicar mapeamento
                original_values = decoded_data[column].astype(str)
                decoded_data[column] = original_values.map(dict_map).fillna(original_values)
        
        # Aplicar dicionários de relacionamento (apenas se necessário)
        rel_columns = [col for col in decoded_data.columns if col.startswith('REL_') and col not in ['REL_TRAB', 'REL_CAT']]
        rel_dict = {'1': 'Sim', '2': 'Não', '9': 'Ignorado', '': 'Branco'}
        for col in rel_columns[:5]:  # Limitar a 5 colunas para não sobrecarregar
            if col in decoded_data.columns:
                print(f"   [INFO] Aplicando dicionario em {col}")
                decoded_data[col] = decoded_data[col].astype(str).replace(rel_dict)
        
        print("[OK] Dicionarios aplicados")
        return decoded_data
    
    def filter_comprehensive_violence(self, data, already_filtered_by_age=False):
        """
        Filtra todos os tipos de violência contra crianças e adolescentes (0-17 anos)
        
        Args:
            data: DataFrame com os dados
            already_filtered_by_age: Se True, assume que os dados já foram filtrados por idade
        """
        print("[INFO] Filtrando violencia contra criancas e adolescentes...")
        
        original_count = len(data)
        
        # Se os dados já foram filtrados por idade, pular essa etapa
        if already_filtered_by_age:
            print("   ℹ️ Dados já filtrados por idade anteriormente")
            child_data = data
        else:
            # Definir idades de crianças e adolescentes
            child_ages = [
                'menor de 01 ano', '01 ano', '02 anos', '03 anos', '04 anos', '05 anos',
                '06 anos', '07 anos', '08 anos', '09 anos', '10 anos', '11 anos',
                '12 anos', '13 anos', '14 anos', '15 anos', '16 anos', '17 anos'
            ]
            
            # Filtrar por idade
            if 'NU_IDADE_N' in data.columns:
                print("   [INFO] Filtrando por idade (0-17 anos)...")
                age_filter = data['NU_IDADE_N'].isin(child_ages)
                child_data = data[age_filter]
                print(f"   [OK] Casos de 0-17 anos: {len(child_data):,}")
            else:
                print("   [AVISO] Coluna NU_IDADE_N nao encontrada")
                child_data = data
        
        # Filtrar por todos os tipos de violência
        # Os valores estão como '1' (Sim), '2' (Não), '9' (Ignorado), '' (Branco)
        violence_columns = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
        available_violence_cols = [col for col in violence_columns if col in child_data.columns]
        
        if available_violence_cols:
            print(f"   [INFO] Filtrando por violencia: {available_violence_cols}")
            
            # Valores que indicam violência: '1', 'Sim', 'SIM', 'S', 'sim', 1 (numérico)
            violence_values = ['1', 'Sim', 'SIM', 'S', 'sim', '1.0']
            
            # Criar condição para qualquer tipo de violência
            violence_conditions = []
            for col in available_violence_cols:
                # Converter para string e verificar
                col_str = child_data[col].astype(str)
                col_condition = col_str.isin(violence_values)
                
                # Também verificar se é numérico 1 (se for numérico)
                if child_data[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                    col_num = (child_data[col] == 1)
                    col_condition = col_condition | col_num
                
                violence_conditions.append(col_condition)
            
            # Combinar condições com OR
            combined_condition = violence_conditions[0]
            for condition in violence_conditions[1:]:
                combined_condition = combined_condition | condition
            
            violence_data = child_data[combined_condition]
            
            # Estatísticas por tipo
            for col in available_violence_cols:
                col_str = child_data[col].astype(str)
                count_str = col_str.isin(violence_values).sum()
                if child_data[col].dtype in ['int64', 'float64']:
                    count_num = (child_data[col] == 1).sum()
                else:
                    count_num = 0
                count = max(count_str, count_num)
                print(f"   [INFO] {col}: {count:,} casos")
            
            filtered_count = len(violence_data)
            print(f"   [OK] Casos de violencia infantil: {filtered_count:,}")
            
        else:
            print("   [AVISO] Nenhuma coluna de violencia encontrada")
            violence_data = child_data
            filtered_count = len(violence_data)
        
        # Evitar divisão por zero
        if original_count > 0:
            percentage = (filtered_count/original_count*100)
            print(f"[OK] Filtrado: {filtered_count:,} casos de {original_count:,} ({percentage:.1f}%)")
        else:
            print(f"[OK] Filtrado: {filtered_count:,} casos (dados ja filtrados anteriormente)")
        
        return violence_data
    
    def create_faixa_etaria(self, data):
        """
        Cria coluna FAIXA_ETARIA agrupando idades em faixas (0-1, 2-5, 6-9, 10-13, 14-17)
        
        Args:
            data: DataFrame com coluna NU_IDADE_N
            
        Returns:
            DataFrame com coluna FAIXA_ETARIA adicionada
        """
        if 'NU_IDADE_N' not in data.columns:
            print("   [AVISO] Coluna NU_IDADE_N nao encontrada para criar FAIXA_ETARIA")
            return data
        
        def map_idade_to_faixa(idade_str):
            """Mapeia idade para faixa etária"""
            if pd.isna(idade_str) or idade_str == '':
                return 'Não informado'
            
            idade_str = str(idade_str).strip()
            
            # Mapear idades decodificadas para faixas etárias
            if idade_str in ['menor de 01 ano', 'menor de 1 ano']:
                return '0-1 anos'
            elif idade_str in ['01 ano', '1 ano']:
                return '0-1 anos'
            elif idade_str in ['02 anos', '03 anos', '04 anos', '05 anos', '2 anos', '3 anos', '4 anos', '5 anos']:
                # Extrair número da idade
                idade_num = None
                for i in range(2, 6):
                    if idade_str.startswith(f'{i:02d} anos') or idade_str.startswith(f'{i} anos'):
                        idade_num = i
                        break
                
                if idade_num is None:
                    return 'Não informado'
                
                if idade_num in [2, 3, 4, 5]:
                    return '2-5 anos'
            elif idade_str in ['06 anos', '07 anos', '08 anos', '09 anos', '6 anos', '7 anos', '8 anos', '9 anos']:
                # Extrair número da idade
                idade_num = None
                for i in range(6, 10):
                    if idade_str.startswith(f'{i:02d} anos') or idade_str.startswith(f'{i} anos'):
                        idade_num = i
                        break
                
                if idade_num is None:
                    return 'Não informado'
                
                if idade_num in [6, 7, 8, 9]:
                    return '6-9 anos'
            elif idade_str in ['10 anos', '11 anos', '12 anos', '13 anos']:
                # Extrair número da idade
                idade_num = None
                for i in range(10, 14):
                    if idade_str.startswith(f'{i:02d} anos') or idade_str.startswith(f'{i} anos'):
                        idade_num = i
                        break
                
                if idade_num is None:
                    return 'Não informado'
                
                if idade_num in [10, 11, 12, 13]:
                    return '10-13 anos'
            elif idade_str in ['14 anos', '15 anos', '16 anos', '17 anos']:
                # Extrair número da idade
                idade_num = None
                for i in range(14, 18):
                    if idade_str.startswith(f'{i:02d} anos') or idade_str.startswith(f'{i} anos'):
                        idade_num = i
                        break
                
                if idade_num is None:
                    return 'Não informado'
                
                if idade_num in [14, 15, 16, 17]:
                    return '14-17 anos'
            
            return 'Não informado'
        
        data = data.copy()
        data['FAIXA_ETARIA'] = data['NU_IDADE_N'].apply(map_idade_to_faixa)
        
        print(f"   [OK] Coluna FAIXA_ETARIA criada")
        print(f"   [INFO] Distribuicao: {data['FAIXA_ETARIA'].value_counts().to_dict()}")
        
        return data
    
    def load_population_data(self):
        """
        Carrega dados populacionais
        """
        print("[INFO] Carregando dados populacionais...")
        
        try:
            parquet_files = list(self.population_data_path.glob("*.parquet"))
            if parquet_files:
                all_data = []
                for file_path in parquet_files:
                    df = pd.read_parquet(file_path)
                    all_data.append(df)
                
                self.population_data = pd.concat(all_data, ignore_index=True)
                print(f"[OK] Dados populacionais carregados: {len(self.population_data):,} registros")
            else:
                print("[AVISO] Nenhum arquivo populacional encontrado")
                self.population_data = None
                
        except Exception as e:
            print(f"[ERRO] Erro ao carregar dados populacionais: {e}")
            self.population_data = None
            
        return self.population_data
    
    def generate_comprehensive_statistics(self, data):
        """
        Gera estatísticas completas incluindo análises socioeconômicas
        """
        print("[INFO] Gerando estatisticas completas...")
        
        stats = {
            'total_cases': len(data),
            'temporal': {},
            'demographic': {},
            'socioeconomic': {},
            'violence_types': {},
            'aggressor_profile': {},
            'location_analysis': {},
            'access_services': {}
        }
        
        # Análise temporal
        if 'DT_NOTIFIC' in data.columns:
            data['DT_NOTIFIC'] = pd.to_datetime(data['DT_NOTIFIC'], errors='coerce')
            stats['temporal'] = {
                'by_year': data['DT_NOTIFIC'].dt.year.value_counts().to_dict(),
                'by_month': data['DT_NOTIFIC'].dt.month.value_counts().to_dict()
            }
        
        # Análise demográfica
        if 'CS_SEXO' in data.columns:
            stats['demographic']['sex'] = data['CS_SEXO'].value_counts().to_dict()
        
        if 'NU_IDADE_N' in data.columns:
            stats['demographic']['age'] = data['NU_IDADE_N'].value_counts().to_dict()
        
        # Análise socioeconômica
        socioeconomic_cols = ['CS_ESCOL_N', 'CS_RACA', 'ID_OCUPA_N', 'SIT_CONJUG']
        for col in socioeconomic_cols:
            if col in data.columns:
                stats['socioeconomic'][col] = data[col].value_counts().head(10).to_dict()
        
        # Análise de tipos de violência
        violence_cols = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
        for col in violence_cols:
            if col in data.columns:
                stats['violence_types'][col] = data[col].value_counts().to_dict()
        
        # Perfil do agressor
        if 'AUTOR_SEXO' in data.columns:
            stats['aggressor_profile']['sex'] = data['AUTOR_SEXO'].value_counts().to_dict()
        
        if 'AUTOR_ALCO' in data.columns:
            stats['aggressor_profile']['alcohol'] = data['AUTOR_ALCO'].value_counts().to_dict()
        
        # Relação com agressor
        rel_cols = [col for col in data.columns if col.startswith('REL_')]
        for col in rel_cols:
            if col in data.columns:
                rel_data = data[col].value_counts()
                if len(rel_data) > 0:
                    stats['aggressor_profile'][col] = rel_data.head(5).to_dict()
        
        # Análise de local
        if 'LOCAL_OCOR' in data.columns:
            stats['location_analysis']['main_location'] = data['LOCAL_OCOR'].value_counts().to_dict()
        
        if 'LOCAL_ESPE' in data.columns:
            stats['location_analysis']['specific_location'] = data['LOCAL_ESPE'].value_counts().head(10).to_dict()
        
        # Acesso a serviços
        if 'REDE_SAU' in data.columns:
            stats['access_services']['health'] = data['REDE_SAU'].value_counts().to_dict()
        
        if 'REDE_EDUCA' in data.columns:
            stats['access_services']['education'] = data['REDE_EDUCA'].value_counts().to_dict()
        
        print("[OK] Estatisticas geradas")
        return stats
    
    def process_all_data(self):
        """
        Processa todos os dados de forma completa
        """
        print("[INFO] Iniciando processamento completo...")
        start_time = time.time()
        
        # 1. Carregar dicionários
        self.load_dictionaries()
        
        # 2. Carregar dados de violência
        violence_data = self.load_violence_data()
        
        # 3. Aplicar dicionários
        decoded_data = self.apply_dictionaries(violence_data)
        
        # 4. Filtrar violência contra crianças
        child_data = self.filter_comprehensive_violence(decoded_data)
        
        # 5. Carregar dados populacionais
        population_data = self.load_population_data()
        
        # 6. Gerar estatísticas
        stats = self.generate_comprehensive_statistics(child_data)
        
        # 7. Salvar dados processados
        self.processed_data = child_data
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"[OK] Processamento completo finalizado em {processing_time:.2f} segundos")
        print(f"[INFO] Dados processados: {len(child_data):,} casos")
        
        return child_data, population_data, stats
    
    def get_analysis_summary(self):
        """
        Retorna resumo da análise
        """
        if self.processed_data is None:
            return "Nenhum dado processado"
        
        data = self.processed_data
        
        summary = {
            'total_cases': len(data),
            'violence_types': {},
            'demographic_profile': {},
            'socioeconomic_profile': {},
            'aggressor_profile': {},
            'location_profile': {}
        }
        
        # Tipos de violência
        violence_cols = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
        for col in violence_cols:
            if col in data.columns:
                count = len(data[data[col].isin(['Sim', '1', 'S', 'SIM', 'sim'])])
                summary['violence_types'][col] = count
        
        # Perfil demográfico
        if 'CS_SEXO' in data.columns:
            summary['demographic_profile']['sex'] = data['CS_SEXO'].value_counts().to_dict()
        
        if 'NU_IDADE_N' in data.columns:
            summary['demographic_profile']['age_groups'] = data['NU_IDADE_N'].value_counts().head(5).to_dict()
        
        # Perfil socioeconômico
        if 'CS_ESCOL_N' in data.columns:
            summary['socioeconomic_profile']['education'] = data['CS_ESCOL_N'].value_counts().head(5).to_dict()
        
        if 'CS_RACA' in data.columns:
            summary['socioeconomic_profile']['race'] = data['CS_RACA'].value_counts().to_dict()
        
        # Perfil do agressor
        if 'AUTOR_SEXO' in data.columns:
            summary['aggressor_profile']['sex'] = data['AUTOR_SEXO'].value_counts().to_dict()
        
        # Local de ocorrência
        if 'LOCAL_OCOR' in data.columns:
            summary['location_profile']['main_location'] = data['LOCAL_OCOR'].value_counts().head(5).to_dict()
        
        return summary

    
    def create_derived_columns(self, df):
        """
        Cria todas as colunas derivadas necessárias para o dashboard.
        Centraliza a lógica que antes estava espalhada no dashboard.
        """
        print("[INFO] Criando colunas derivadas (ETL)...")
        # Evitar SettingWithCopyWarning
        df = df.copy()
        
        # 1. Processar Datas e Ano
        if 'DT_NOTIFIC' in df.columns:
            # Tentar converter apenas se não for datetime
            if not pd.api.types.is_datetime64_any_dtype(df['DT_NOTIFIC']):
                df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'], errors='coerce')
            df['ANO_NOTIFIC'] = df['DT_NOTIFIC'].dt.year
        elif 'NU_ANO' in df.columns:
            df['ANO_NOTIFIC'] = pd.to_numeric(df['NU_ANO'], errors='coerce')
            
        # 2. Faixa Etária (Reusar logica existente ou aprimorada)
        # O dashboard usa uma logica complexa de regex. O processor ja tem create_faixa_etaria.
        # Vamos garantir que create_faixa_etaria seja chamada.
        if 'FAIXA_ETARIA' not in df.columns:
            df = self.create_faixa_etaria(df)
            
        # 3. UF de Notificação (Nome)
        uf_dict = {
            '11': 'Rondônia', '12': 'Acre', '13': 'Amazonas', '14': 'Roraima',
            '15': 'Pará', '16': 'Amapá', '17': 'Tocantins', '21': 'Maranhão',
            '22': 'Piauí', '23': 'Ceará', '24': 'Rio Grande do Norte', '25': 'Paraíba',
            '26': 'Pernambuco', '27': 'Alagoas', '28': 'Sergipe', '29': 'Bahia',
            '31': 'Minas Gerais', '32': 'Espírito Santo', '33': 'Rio de Janeiro',
            '35': 'São Paulo', '41': 'Paraná', '42': 'Santa Catarina', '43': 'Rio Grande do Sul',
            '50': 'Mato Grosso do Sul', '51': 'Mato Grosso', '52': 'Goiás', '53': 'Distrito Federal'
        }
        
        def map_uf(val):
            if pd.isna(val): return 'Não informado'
            val_str = str(val).strip()
            # Tentar direto
            if val_str in uf_dict: return uf_dict[val_str]
            # Tentar float->int->str
            try:
                if val_str.replace('.','',1).isdigit():
                    val_int = str(int(float(val_str)))
                    if val_int in uf_dict: return uf_dict[val_int]
            except: pass
            return val_str
            
        if 'SG_UF_NOT' in df.columns:
            print("   [INFO] Mapeando UFs...")
            # Otimização: usar map que é muito mais rapido que apply para dicionarios
            # Mas map nao lida bem com a logica de fallback complexa. Apply é seguro.
            df['UF_NOTIFIC'] = df['SG_UF_NOT'].apply(map_uf)
        elif 'SG_UF' in df.columns:
            df['UF_NOTIFIC'] = df['SG_UF'].apply(map_uf)
        else:
            df['UF_NOTIFIC'] = 'N/A'
            
        # 4. Município (Nome)
        # Importar aqui para evitar ciclo se necessario, ou assumir que utils existe
        try:
            from src.utils.munic_dict_loader import load_municipality_dict
            municip_dict = load_municipality_dict()
            
            print("   [INFO] Mapeando Municipios...")
            def map_municipio(codigo):
                if pd.isna(codigo): return 'Não informado'
                c = str(codigo).strip()
                if len(c) == 6 and c in municip_dict: return municip_dict[c]
                return c
            
            col_munic = 'ID_MUNICIP' if 'ID_MUNICIP' in df.columns else 'ID_MN_RESI' if 'ID_MN_RESI' in df.columns else None
            if col_munic:
                df['MUNICIPIO_NOTIFIC'] = df[col_munic].apply(map_municipio)
            else:
                df['MUNICIPIO_NOTIFIC'] = 'N/A'
        except ImportError:
            print("[AVISO] Nao foi possivel carregar dicionario de municipios. Pulando.")
            df['MUNICIPIO_NOTIFIC'] = df['ID_MUNICIP'] if 'ID_MUNICIP' in df.columns else 'N/A'

        # 5. Tipo de Violencia (Consolidado)
        print("   [INFO] Consolidando Tipos de Violencia...")
        def get_violence_type(row):
            types = []
            # Assumindo que ja passou por apply_dictionaries e os valores sao 'Sim'/'Não' etc
            # Mas o apply_dictionaries original pode ter mantido codigos se nao achou.
            # O processor.apply_dictionaries mapeia '1'->'Sim'.
            # Vamos checar 'Sim', '1', 'S'.
            
            check = ['Sim', 'SIM', '1', 'S', '1.0']
            
            if str(row.get('VIOL_FISIC', '')).strip() in check: types.append('Física')
            if str(row.get('VIOL_PSICO', '')).strip() in check: types.append('Psicológica')
            if str(row.get('VIOL_SEXU', '')).strip() in check: types.append('Sexual')
            if str(row.get('VIOL_INFAN', '')).strip() in check: types.append('Infantil')
            
            if not types: types.append('Não especificado')
            return ', '.join(types)

        df['TIPO_VIOLENCIA'] = df.apply(get_violence_type, axis=1)
        
        # 6. Sexo (Simples)
        if 'CS_SEXO' in df.columns:
            def map_sex(val):
                s = str(val).strip().upper()
                if s in ['1', 'M', 'MASCULINO']: return 'Masculino'
                if s in ['2', 'F', 'FEMININO']: return 'Feminino'
                return 'Ignorado'
            
            df['SEXO'] = df['CS_SEXO'].apply(map_sex)
            
        # 7. Tempo Ocorrencia-Denuncia
        if 'DT_OCOR' in df.columns and 'DT_NOTIFIC' in df.columns:
            try:
                # Converter para datetime se nao for
                if not pd.api.types.is_datetime64_any_dtype(df['DT_OCOR']):
                    df['DT_OCOR'] = pd.to_datetime(df['DT_OCOR'], errors='coerce')
                
                mask_validas = df['DT_NOTIFIC'].notna() & df['DT_OCOR'].notna()
                df['TEMPO_OCOR_DENUNCIA'] = None
                if mask_validas.any():
                    df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = (df.loc[mask_validas, 'DT_NOTIFIC'] - df.loc[mask_validas, 'DT_OCOR']).dt.days
                    # Limpar invalidos
                    df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'].apply(lambda x: x if pd.notna(x) and 0 <= x <= 3650 else None)
            except Exception as e:
                print(f"[AVISO] Erro ao calcular TEMPO_OCOR_DENUNCIA: {e}")
                df['TEMPO_OCOR_DENUNCIA'] = None
                
        # 8. Encaminhamentos Justica
        enc_cols = ['ENC_DELEG', 'ENC_DPCA', 'ENC_MPU', 'ENC_VARA']
        if any(c in df.columns for c in enc_cols):
            def get_enc(row):
                encs = []
                check = ['1', 'SIM', 'S', '1.0']
                for c in enc_cols:
                    if c in row.index and pd.notna(row[c]):
                        val = str(row[c]).upper().strip()
                        if val in check or (val.isdigit() and int(val) == 1):
                            if c == 'ENC_DELEG': encs.append('Delegacia')
                            elif c == 'ENC_DPCA': encs.append('DPCA')
                            elif c == 'ENC_MPU': encs.append('Min. Público')
                            elif c == 'ENC_VARA': encs.append('Vara Infância')
                return ', '.join(encs) if encs else 'Nenhum'
            df['ENCAMINHAMENTOS_JUSTICA'] = df.apply(get_enc, axis=1)
        else:
            df['ENCAMINHAMENTOS_JUSTICA'] = 'Não informado'
            
        # 9. Autor Sexo Corrigido
        if 'AUTOR_SEXO' in df.columns:
            def map_as(val):
                if pd.isna(val): return 'Não informado'
                s = str(val).upper().strip()
                if s in ['1', 'M', 'MASCULINO']: return 'Masculino'
                if s in ['2', 'F', 'FEMININO']: return 'Feminino'
                if s in ['3', 'OUTROS']: return 'Outros'
                return 'Não informado'
            df['AUTOR_SEXO_CORRIGIDO'] = df['AUTOR_SEXO'].apply(map_as)
        else:
             df['AUTOR_SEXO_CORRIGIDO'] = 'Não informado'
             
        # 10. Grau Parentesco
        rel_cols = [c for c in df.columns if c.startswith('REL_') and c not in ['REL_TRAB', 'REL_CAT']]
        
        # Dicionário de Mapeamento Completo
        RELACIONAMENTO_DICT = {
            'REL_PAI': 'Pai',
            'REL_MAE': 'Mãe',
            'REL_PAD': 'Padrasto',
            'REL_MAD': 'Madrasta',
            'REL_CONJ': 'Cônjuge',
            'REL_EXCON': 'Ex-cônjuge',
            'REL_NAMO': 'Namorado(a)',
            'REL_EXNAM': 'Ex-namorado(a)',
            'REL_FILHO': 'Filho(a)',
            'REL_IRMAO': 'Irmão(ã)',
            'REL_AMIGO': 'Amigo(a)/Conhecido', # Agrupando se necessario ou separando
            'REL_CONHEC': 'Conhecido',
            'REL_DESCON': 'Desconhecido',
            'REL_CUIDAD': 'Cuidador(a)',
            'REL_PATRAO': 'Patrão/Chefe',
            'REL_INST': 'Institucional',
            'REL_POL': 'Policial/Agente',
            'REL_PROPRI': 'Própria Pessoa',
            'REL_OUTROS': 'Outros'
        }
        
        if rel_cols:
            def get_rel(row):
                rels = []
                check = ['1', 'SIM', 'S', '1.0']
                for c in rel_cols:
                    if c in row.index and pd.notna(row[c]):
                        val = str(row[c]).upper().strip()
                        if val in check:
                            # Prioridade: Dicionário
                            if c in RELACIONAMENTO_DICT:
                                r = RELACIONAMENTO_DICT[c]
                            else:
                                # Fallback: Nome legivel
                                r = c.replace('REL_', '').replace('_', ' ').title()
                                # Ajustes comuns para colunas nao mapeadas
                                if 'Pai' in r: r = 'Pai'
                                elif 'Mae' in r: r = 'Mãe'
                                elif 'Padr' in r: r = 'Padrasto'
                                elif 'Madr' in r: r = 'Madrasta'
                                elif 'Conjug' in r: r = 'Cônjuge'
                                elif 'Exnam' in r: r = 'Ex-namorado(a)'
                                elif 'Namor' in r: r = 'Namorado(a)'
                                elif 'Amig' in r: r = 'Amigo(a)'
                                elif 'Descon' in r: r = 'Desconhecido'
                            
                            if r not in rels:
                                rels.append(r)
                return ', '.join(rels) if rels else 'Não informado'
            df['GRAU_PARENTESCO'] = df.apply(get_rel, axis=1)
        else:
            df['GRAU_PARENTESCO'] = 'Não informado'
        
        print(f"[OK] Colunas derivadas criadas. Cols: {len(df.columns)}")
        return df

if __name__ == "__main__":
    # Teste do processador
    processor = SINANDataProcessorComprehensive()
    # child_data, population_data, stats = processor.process_all_data()
    # ...

