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
        print("📚 Carregando dicionários básicos...")
        
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
        
        print(f"✅ {len(self.dictionaries)} dicionários carregados")
        
    def load_violence_data(self):
        """
        Carrega dados de violência de todos os arquivos Parquet
        """
        print("📊 Carregando dados de violência...")
        
        parquet_files = list(self.violence_data_path.glob("*.parquet"))
        print(f"   📁 Encontrados {len(parquet_files)} arquivos")
        
        all_data = []
        
        for file_path in parquet_files:
            try:
                df = pd.read_parquet(file_path)
                all_data.append(df)
                print(f"   ✅ {file_path.name}: {len(df):,} registros")
            except Exception as e:
                print(f"   ⚠️ Erro ao carregar {file_path.name}: {e}")
        
        if all_data:
            self.violence_data = pd.concat(all_data, ignore_index=True)
            print(f"✅ Total de registros carregados: {len(self.violence_data):,}")
        else:
            print("❌ Nenhum dado carregado")
            
        return self.violence_data
    
    def apply_dictionaries(self, data, columns_to_decode=None):
        """
        Aplica todos os dicionários aos dados (otimizado para grandes volumes)
        """
        print("🔄 Aplicando dicionários...")
        
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
                print(f"   🔄 Aplicando {dict_name} em {column}")
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
                print(f"   🔄 Aplicando dicionário em {col}")
                decoded_data[col] = decoded_data[col].astype(str).replace(rel_dict)
        
        print("✅ Dicionários aplicados")
        return decoded_data
    
    def filter_comprehensive_violence(self, data, already_filtered_by_age=False):
        """
        Filtra todos os tipos de violência contra crianças e adolescentes (0-17 anos)
        
        Args:
            data: DataFrame com os dados
            already_filtered_by_age: Se True, assume que os dados já foram filtrados por idade
        """
        print("🔍 Filtrando violência contra crianças e adolescentes...")
        
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
                print("   👶 Filtrando por idade (0-17 anos)...")
                age_filter = data['NU_IDADE_N'].isin(child_ages)
                child_data = data[age_filter]
                print(f"   ✅ Casos de 0-17 anos: {len(child_data):,}")
            else:
                print("   ⚠️ Coluna NU_IDADE_N não encontrada")
                child_data = data
        
        # Filtrar por todos os tipos de violência
        # Os valores estão como '1' (Sim), '2' (Não), '9' (Ignorado), '' (Branco)
        violence_columns = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
        available_violence_cols = [col for col in violence_columns if col in child_data.columns]
        
        if available_violence_cols:
            print(f"   🚨 Filtrando por violência: {available_violence_cols}")
            
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
                print(f"   📊 {col}: {count:,} casos")
            
            filtered_count = len(violence_data)
            print(f"   ✅ Casos de violência infantil: {filtered_count:,}")
            
        else:
            print("   ⚠️ Nenhuma coluna de violência encontrada")
            violence_data = child_data
            filtered_count = len(violence_data)
        
        # Evitar divisão por zero
        if original_count > 0:
            percentage = (filtered_count/original_count*100)
            print(f"✅ Filtrado: {filtered_count:,} casos de {original_count:,} ({percentage:.1f}%)")
        else:
            print(f"✅ Filtrado: {filtered_count:,} casos (dados já filtrados anteriormente)")
        
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
            print("   ⚠️ Coluna NU_IDADE_N não encontrada para criar FAIXA_ETARIA")
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
        
        print(f"   ✅ Coluna FAIXA_ETARIA criada")
        print(f"   📊 Distribuição: {data['FAIXA_ETARIA'].value_counts().to_dict()}")
        
        return data
    
    def load_population_data(self):
        """
        Carrega dados populacionais
        """
        print("👥 Carregando dados populacionais...")
        
        try:
            parquet_files = list(self.population_data_path.glob("*.parquet"))
            if parquet_files:
                all_data = []
                for file_path in parquet_files:
                    df = pd.read_parquet(file_path)
                    all_data.append(df)
                
                self.population_data = pd.concat(all_data, ignore_index=True)
                print(f"✅ Dados populacionais carregados: {len(self.population_data):,} registros")
            else:
                print("⚠️ Nenhum arquivo populacional encontrado")
                self.population_data = None
                
        except Exception as e:
            print(f"❌ Erro ao carregar dados populacionais: {e}")
            self.population_data = None
            
        return self.population_data
    
    def generate_comprehensive_statistics(self, data):
        """
        Gera estatísticas completas incluindo análises socioeconômicas
        """
        print("📊 Gerando estatísticas completas...")
        
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
        
        print("✅ Estatísticas geradas")
        return stats
    
    def process_all_data(self):
        """
        Processa todos os dados de forma completa
        """
        print("🚀 Iniciando processamento completo...")
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
        
        print(f"✅ Processamento completo finalizado em {processing_time:.2f} segundos")
        print(f"📊 Dados processados: {len(child_data):,} casos")
        
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

if __name__ == "__main__":
    # Teste do processador
    processor = SINANDataProcessorComprehensive()
    child_data, population_data, stats = processor.process_all_data()
    
    # Mostrar resumo
    summary = processor.get_analysis_summary()
    print("\n📊 RESUMO DA ANÁLISE:")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
