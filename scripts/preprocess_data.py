#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Pré-processamento de Dados SINAN
Gera arquivos Parquet otimizados com dados já processados para uso no dashboard.

Execute: python scripts/preprocess_data.py
"""

import sys
from pathlib import Path

# Adicionar diretório raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import re
from src.processors.sinan_data_processor_comprehensive import SINANDataProcessorComprehensive
from src.utils.munic_dict_loader import load_municipality_dict

# Dicionário completo de relacionamentos com agressor (mesmo do dashboard)
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
    'REL_SEXUAL': 'Parceiro Sexual',
    'REL_DESCO': 'Desconhecido',
    'REL_CONHEC': 'Conhecido',
    'REL_CUIDA': 'Cuidador(a)',
    'REL_PATRAO': 'Patrão/Chefe',
    'REL_POL': 'Policial',
    'REL_INST': 'Funcionário de Instituição',
    'REL_PROPRI': 'Próprio/Autoagressão',
    'REL_OUTROS': 'Outros',
    'REL_ESPEC': 'Específico'
}

def create_derived_columns(df):
    """
    Cria todas as colunas derivadas necessárias para o dashboard
    Esta função replica exatamente a lógica do dashboard
    """
    # Função auxiliar para processar datas
    def process_date_column(df, col_name):
        """Processa coluna de data no formato YYYYMMDD"""
        if col_name in df.columns:
            try:
                # Primeiro, tentar converter strings vazias ou espaços para None
                df[col_name] = df[col_name].replace(['', '        ', ' '], None)
                
                # Tentar formato YYYYMMDD primeiro (formato mais comum no SINAN)
                # Converter para string primeiro para garantir formato
                df[col_name] = df[col_name].astype(str).replace(['nan', 'None', 'NaT'], None)
                df[col_name] = pd.to_datetime(df[col_name], format='%Y%m%d', errors='coerce')
            except Exception as e:
                try:
                    # Tentar outros formatos
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                except:
                    # Se falhar, manter como está
                    pass
        return df
    
    # Processar todas as colunas de data
    date_columns = ['DT_NOTIFIC', 'DT_OCOR', 'DT_ENCERRA', 'DT_DIGITA', 'DT_INVEST']
    for col in date_columns:
        df = process_date_column(df, col)
    
    # Extrair ano da data de notificação
    if 'DT_NOTIFIC' in df.columns:
        df['ANO_NOTIFIC'] = df['DT_NOTIFIC'].dt.year
    elif 'NU_ANO' in df.columns:
        df['ANO_NOTIFIC'] = pd.to_numeric(df['NU_ANO'], errors='coerce')
    else:
        df['ANO_NOTIFIC'] = None
    
    # Calcular tempo entre ocorrência e denúncia (em dias)
    if 'DT_OCOR' in df.columns and 'DT_NOTIFIC' in df.columns:
        mask_validas = df['DT_NOTIFIC'].notna() & df['DT_OCOR'].notna()
        df['TEMPO_OCOR_DENUNCIA'] = None
        if mask_validas.any():
            df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = (
                df.loc[mask_validas, 'DT_NOTIFIC'] - df.loc[mask_validas, 'DT_OCOR']
            ).dt.days
            df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'].apply(
                lambda x: x if pd.notna(x) and 0 <= x <= 3650 else None
            )
    
    # Processar encaminhamentos para justiça
    encaminhamento_cols = ['ENC_DELEG', 'ENC_DPCA', 'ENC_MPU', 'ENC_VARA']
    if any(col in df.columns for col in encaminhamento_cols):
        def get_encaminhamentos_justica(row):
            encaminhamentos = []
            for col in encaminhamento_cols:
                if col in row.index and pd.notna(row[col]):
                    val = str(row[col]).upper().strip()
                    if val in ['1', 'SIM', 'S', '1.0'] or (val.isdigit() and int(val) == 1):
                        if col == 'ENC_DELEG':
                            encaminhamentos.append('Delegacia')
                        elif col == 'ENC_DPCA':
                            encaminhamentos.append('DPCA')
                        elif col == 'ENC_MPU':
                            encaminhamentos.append('Ministério Público')
                        elif col == 'ENC_VARA':
                            encaminhamentos.append('Vara da Infância')
            return ', '.join(encaminhamentos) if encaminhamentos else 'Nenhum'
        
        df['ENCAMINHAMENTOS_JUSTICA'] = df.apply(get_encaminhamentos_justica, axis=1)
    else:
        df['ENCAMINHAMENTOS_JUSTICA'] = 'Não informado'
    
    # Dicionário de códigos IBGE para nomes de estados
    uf_dict = {
        '11': 'Rondônia', '12': 'Acre', '13': 'Amazonas', '14': 'Roraima',
        '15': 'Pará', '16': 'Amapá', '17': 'Tocantins', '21': 'Maranhão',
        '22': 'Piauí', '23': 'Ceará', '24': 'Rio Grande do Norte', '25': 'Paraíba',
        '26': 'Pernambuco', '27': 'Alagoas', '28': 'Sergipe', '29': 'Bahia',
        '31': 'Minas Gerais', '32': 'Espírito Santo', '33': 'Rio de Janeiro',
        '35': 'São Paulo', '41': 'Paraná', '42': 'Santa Catarina', '43': 'Rio Grande do Sul',
        '50': 'Mato Grosso do Sul', '51': 'Mato Grosso', '52': 'Goiás', '53': 'Distrito Federal',
        11: 'Rondônia', 12: 'Acre', 13: 'Amazonas', 14: 'Roraima',
        15: 'Pará', 16: 'Amapá', 17: 'Tocantins', 21: 'Maranhão',
        22: 'Piauí', 23: 'Ceará', 24: 'Rio Grande do Norte', 25: 'Paraíba',
        26: 'Pernambuco', 27: 'Alagoas', 28: 'Sergipe', 29: 'Bahia',
        31: 'Minas Gerais', 32: 'Espírito Santo', 33: 'Rio de Janeiro',
        35: 'São Paulo', 41: 'Paraná', 42: 'Santa Catarina', 43: 'Rio Grande do Sul',
        50: 'Mato Grosso do Sul', 51: 'Mato Grosso', 52: 'Goiás', 53: 'Distrito Federal'
    }
    
    # Criar coluna de UF com nomes
    def map_uf(val):
        if pd.isna(val):
            return 'Não informado'
        val_str = str(val).strip()
        if val_str in uf_dict:
            return uf_dict[val_str]
        try:
            val_num = int(float(val))
            if val_num in uf_dict:
                return uf_dict[val_num]
        except:
            pass
        return val_str
    
    if 'SG_UF_NOT' in df.columns:
        df['UF_NOTIFIC'] = df['SG_UF_NOT'].apply(map_uf)
    elif 'SG_UF' in df.columns:
        df['UF_NOTIFIC'] = df['SG_UF'].apply(map_uf)
    else:
        df['UF_NOTIFIC'] = 'N/A'
    
    # Carregar dicionário de municípios
    try:
        config_path = project_root / "data" / "config" / "TAB_SINANONLINE"
        municip_dict = load_municipality_dict(str(config_path))
    except:
        municip_dict = {}
    
    # Criar coluna de município com nomes
    def map_municipio(codigo, municip_dict):
        if pd.isna(codigo):
            return 'Não informado'
        codigo_str = str(codigo).strip()
        if len(codigo_str) == 6 and codigo_str in municip_dict:
            return municip_dict[codigo_str]
        return codigo_str
    
    if 'ID_MUNICIP' in df.columns:
        df['MUNICIPIO_NOTIFIC'] = df['ID_MUNICIP'].apply(lambda x: map_municipio(x, municip_dict))
    elif 'ID_MN_RESI' in df.columns:
        df['MUNICIPIO_NOTIFIC'] = df['ID_MN_RESI'].apply(lambda x: map_municipio(x, municip_dict))
    else:
        df['MUNICIPIO_NOTIFIC'] = 'N/A'
    
    # Criar coluna de tipo de violência
    def get_violence_type(row):
        types = []
        viol_fisic = str(row.get('VIOL_FISIC', '')).upper() if pd.notna(row.get('VIOL_FISIC')) else ''
        viol_psico = str(row.get('VIOL_PSICO', '')).upper() if pd.notna(row.get('VIOL_PSICO')) else ''
        viol_sexu = str(row.get('VIOL_SEXU', '')).upper() if pd.notna(row.get('VIOL_SEXU')) else ''
        viol_infan = str(row.get('VIOL_INFAN', '')).upper() if pd.notna(row.get('VIOL_INFAN')) else ''
        
        if viol_fisic in ['SIM', '1', 'S', '1.0']:
            types.append('Física')
        if viol_psico in ['SIM', '1', 'S', '1.0']:
            types.append('Psicológica')
        if viol_sexu in ['SIM', '1', 'S', '1.0']:
            types.append('Sexual')
        if viol_infan in ['SIM', '1', 'S', '1.0']:
            types.append('Infantil')
        if not types:
            types.append('Não especificado')
        return ', '.join(types)
    
    df['TIPO_VIOLENCIA'] = df.apply(get_violence_type, axis=1)
    
    # Criar faixa etária (0-17 anos: 0-1, 2-5, 6-9, 10-13, 14-17)
    if 'NU_IDADE_N' in df.columns:
        def get_age_group(age_str):
            if pd.isna(age_str):
                return 'Não informado'
            age_str = str(age_str).strip()
            
            idade_num = None
            
            if 'menor de' in age_str.lower() or age_str.lower() in ['menor de 01 ano', 'menor de 1 ano']:
                return '0-1 anos'
            
            # Tentar extrair número da idade usando regex (mais robusto)
            match = re.search(r'(\d{1,2})\s*ano', age_str, re.IGNORECASE)
            if match:
                idade_num = int(match.group(1))
            else:
                # Fallback: verificar strings conhecidas
                if age_str in ['01 ano', '1 ano']:
                    idade_num = 1
                elif age_str in ['02 anos', '2 anos']:
                    idade_num = 2
                elif age_str in ['03 anos', '3 anos']:
                    idade_num = 3
                elif age_str in ['04 anos', '4 anos']:
                    idade_num = 4
                elif age_str in ['05 anos', '5 anos']:
                    idade_num = 5
                elif age_str in ['06 anos', '6 anos']:
                    idade_num = 6
                elif age_str in ['07 anos', '7 anos']:
                    idade_num = 7
                elif age_str in ['08 anos', '8 anos']:
                    idade_num = 8
                elif age_str in ['09 anos', '9 anos']:
                    idade_num = 9
                elif age_str in ['10 anos']:
                    idade_num = 10
                elif age_str in ['11 anos']:
                    idade_num = 11
                elif age_str in ['12 anos']:
                    idade_num = 12
                elif age_str in ['13 anos']:
                    idade_num = 13
                elif age_str in ['14 anos']:
                    idade_num = 14
                elif age_str in ['15 anos']:
                    idade_num = 15
                elif age_str in ['16 anos']:
                    idade_num = 16
                elif age_str in ['17 anos']:
                    idade_num = 17
            
            if idade_num is not None:
                if idade_num == 0 or idade_num == 1:
                    return '0-1 anos'
                elif 2 <= idade_num <= 5:
                    return '2-5 anos'
                elif 6 <= idade_num <= 9:
                    return '6-9 anos'
                elif 10 <= idade_num <= 13:
                    return '10-13 anos'
                elif 14 <= idade_num <= 17:
                    return '14-17 anos'
            
            return 'Não informado'
        
        df['FAIXA_ETARIA'] = df['NU_IDADE_N'].apply(get_age_group)
    else:
        df['FAIXA_ETARIA'] = 'Não informado'
    
    # Criar coluna de sexo
    if 'CS_SEXO' in df.columns:
        def map_sexo(val):
            if pd.isna(val):
                return 'Não informado'
            val_str = str(val).upper().strip()
            if val_str in ['M', '1', 'MASCULINO', 'MAS']:
                return 'Masculino'
            elif val_str in ['F', '2', 'FEMININO', 'FEM']:
                return 'Feminino'
            else:
                return 'Não informado'
        df['SEXO'] = df['CS_SEXO'].apply(map_sexo)
    else:
        df['SEXO'] = 'Não informado'
    
    # Criar coluna de sexo do agressor
    if 'AUTOR_SEXO' in df.columns:
        def map_autor_sexo(val):
            if pd.isna(val):
                return 'Não informado'
            val_str = str(val).upper().strip()
            if val_str in ['1', 'M', 'MASCULINO', 'MAS']:
                return 'Masculino'
            elif val_str in ['2', 'F', 'FEMININO', 'FEM']:
                return 'Feminino'
            elif val_str in ['3', 'OUTROS', 'OUTRO']:
                return 'Outros'
            elif val_str in ['9', 'IGNORADO', 'IGN']:
                return 'Ignorado'
            else:
                return 'Não informado'
        df['AUTOR_SEXO_CORRIGIDO'] = df['AUTOR_SEXO'].apply(map_autor_sexo)
    else:
        df['AUTOR_SEXO_CORRIGIDO'] = 'Não informado'
    
    # Criar coluna de relacionamento com agressor
    rel_cols = [col for col in df.columns if col.startswith('REL_') and col not in ['REL_TRAB', 'REL_CAT']]
    if rel_cols:
        def get_relacionamento(row):
            relacionamentos = []
            for col in rel_cols:
                if col in row.index and pd.notna(row[col]):
                    val = str(row[col]).upper().strip()
                    if val in ['1', 'SIM', 'S', '1.0']:
                        if col in RELACIONAMENTO_DICT:
                            relacionamento = RELACIONAMENTO_DICT[col]
                        else:
                            relacionamento = col.replace('REL_', '').replace('_', ' ').title()
                            relacionamento = relacionamento.replace('Exnam', 'Ex-namorado(a)')
                            relacionamento = relacionamento.replace('Excon', 'Ex-cônjuge')
                            relacionamento = relacionamento.replace('Conhec', 'Conhecido')
                            relacionamento = relacionamento.replace('Propri', 'Próprio/Autoagressão')
                            relacionamento = relacionamento.replace('Cuid', 'Cuidador(a)')
                            relacionamento = relacionamento.replace('Patrao', 'Patrão/Chefe')
                            relacionamento = relacionamento.replace('Inst', 'Funcionário de Instituição')
                        relacionamentos.append(relacionamento)
            return ', '.join(relacionamentos) if relacionamentos else 'Não informado'
        df['GRAU_PARENTESCO'] = df.apply(get_relacionamento, axis=1)
    else:
        df['GRAU_PARENTESCO'] = 'Não informado'
    
    return df

def preprocess_data(filter_violence=True):
    """
    Pré-processa os dados SINAN e salva em formato otimizado
    
    Args:
        filter_violence: Se True, filtra apenas casos com violência marcada.
                        Se False, mantém todos os registros de 0-17 anos (mais inclusivo)
    """
    
    print("=" * 70)
    print("PRE-PROCESSAMENTO DE DADOS SINAN")
    print("=" * 70)
    print()
    
    # Caminhos
    data_dir = project_root / "data"
    raw_dir = data_dir / "raw" / "VIOLBR-PARQUET"
    config_dir = data_dir / "config" / "TAB_SINANONLINE"
    processed_dir = data_dir / "processed"
    
    # Criar diretório de dados processados
    processed_dir.mkdir(exist_ok=True)
    
    # Verificar se os dados brutos existem
    if not raw_dir.exists():
        print(f"[ERRO] Diretorio de dados nao encontrado: {raw_dir}")
        return False
    
    parquet_files = list(raw_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"[ERRO] Nenhum arquivo Parquet encontrado em {raw_dir}")
        return False
    
    print(f"[INFO] Encontrados {len(parquet_files)} arquivos Parquet")
    print()
    
    try:
        # Inicializar processador
        print("[INFO] Inicializando processador de dados...")
        processor = SINANDataProcessorComprehensive(
            violence_data_path=str(raw_dir),
            dict_path=str(config_dir)
        )
        
        # Carregar dicionários
        print("[INFO] Carregando dicionarios SINAN...")
        processor.load_dictionaries()
        print("[OK] Dicionarios carregados")
        print()
        
        # Carregar dados de violência
        print("[INFO] Carregando dados de violencia...")
        violence_data = processor.load_violence_data()
        
        if violence_data is None or len(violence_data) == 0:
            print("[ERRO] Nenhum dado de violencia encontrado")
            return False
        
        print(f"[OK] {len(violence_data):,} registros carregados")
        print()
        
        # Filtrar por idade ANTES de aplicar dicionários (mais eficiente)
        print("[INFO] Filtrando dados para faixa etaria 0-17 anos...")
        if 'NU_IDADE_N' in violence_data.columns:
            # Códigos de idade: 4000 (menor de 1 ano) até 4017 (17 anos)
            # Formato: 4000, 4001, ..., 4009, 4010, 4011, ..., 4017
            age_codes = [f'400{i}' if i < 10 else f'40{i}' for i in range(0, 18)]
            
            age_str = violence_data['NU_IDADE_N'].astype(str)
            age_filter = age_str.isin(age_codes)
            child_data_raw = violence_data[age_filter].copy(deep=False)
            print(f"[OK] {len(child_data_raw):,} registros apos filtro de idade")
            
            del violence_data
        else:
            print("[AVISO] Coluna NU_IDADE_N nao encontrada")
            child_data_raw = violence_data.copy(deep=False)
            del violence_data
        print()
        
        # Aplicar dicionários apenas nos dados filtrados
        print("[INFO] Aplicando dicionarios aos dados...")
        decoded_data = processor.apply_dictionaries(child_data_raw)
        del child_data_raw
        print("[OK] Dicionarios aplicados")
        print()
        
        # Filtrar violência (opcional - pode ser muito restritivo)
        print("[INFO] Filtrando casos de violencia...")
        print(f"[INFO] Registros ANTES do filtro de violencia: {len(decoded_data):,}")
        
        if filter_violence:
            print("[INFO] Aplicando filtro de violencia...")
            child_data = processor.filter_comprehensive_violence(decoded_data, already_filtered_by_age=True)
            del decoded_data
            print(f"[OK] Registros APOS filtro de violencia: {len(child_data):,}")
            
            if len(child_data) == 0:
                print("[ERRO] Nenhum registro encontrado apos filtro de violencia!")
                return False
        else:
            print("[INFO] Pulando filtro de violencia - mantendo todos os registros de 0-17 anos")
            child_data = decoded_data.copy(deep=False)
            del decoded_data
            print(f"[OK] Registros mantidos: {len(child_data):,}")
        
        print()
        
        # Criar TODAS as colunas derivadas (exatamente como o dashboard faz)
        print("[INFO] Criando colunas derivadas (replicando logica do dashboard)...")
        df = create_derived_columns(child_data.copy(deep=False))
        del child_data
        print("[OK] Colunas derivadas criadas")
        print()
        
        # Verificar colunas criadas
        print("[INFO] Colunas criadas:")
        required_cols = ['ANO_NOTIFIC', 'TIPO_VIOLENCIA', 'SEXO', 'UF_NOTIFIC', 'FAIXA_ETARIA', 'MUNICIPIO_NOTIFIC']
        for col in required_cols:
            if col in df.columns:
                print(f"   [OK] {col}")
            else:
                print(f"   [ERRO] {col} - NAO CRIADA!")
        print()
        
        # Otimizar tipos de dados para economizar memória
        print("[INFO] Otimizando tipos de dados...")
        
        # Converter colunas object com poucos valores únicos para category
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() < len(df) * 0.5:
                try:
                    df[col] = df[col].astype('category')
                except:
                    pass
        
        # Converter int64 para int32 quando possível
        for col in df.select_dtypes(include=['int64']).columns:
            try:
                if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                    df[col] = df[col].astype('int32')
            except:
                pass
        
        print("[OK] Tipos de dados otimizados")
        print()
        
        # Salvar dados processados
        output_file = processed_dir / "sinan_data_processed.parquet"
        print(f"[INFO] Salvando dados processados em {output_file}...")
        
        df.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"[OK] Dados salvos! Tamanho: {file_size_mb:.2f} MB")
        print()
        
        # Salvar metadados
        metadata = {
            'total_registros': len(df),
            'colunas': list(df.columns),
            'data_processamento': pd.Timestamp.now().isoformat(),
            'versao': '2.0'
        }
        
        import json
        metadata_file = processed_dir / "metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"[OK] Metadados salvos em {metadata_file}")
        print()
        
        # Estatísticas
        print("=" * 70)
        print("ESTATISTICAS DOS DADOS PROCESSADOS")
        print("=" * 70)
        print(f"Total de registros: {len(df):,}")
        print(f"Total de colunas: {len(df.columns)}")
        print(f"Tamanho do arquivo: {file_size_mb:.2f} MB")
        print()
        
        if 'FAIXA_ETARIA' in df.columns:
            print("Distribuicao por faixa etaria:")
            print(df['FAIXA_ETARIA'].value_counts().head(10))
            print()
        
        if 'SEXO' in df.columns:
            print("Distribuicao por sexo:")
            print(df['SEXO'].value_counts())
            print()
        
        if 'ANO_NOTIFIC' in df.columns:
            print("Distribuicao por ano:")
            print(df['ANO_NOTIFIC'].value_counts().sort_index())
            print()
        
        print("=" * 70)
        print("[OK] PRE-PROCESSAMENTO CONCLUIDO COM SUCESSO!")
        print("=" * 70)
        print()
        print("Os dados estao prontos para uso no dashboard.")
        print("O dashboard agora carregara os dados muito mais rapido!")
        
        return True
        
    except Exception as e:
        print(f"[ERRO] Erro durante pre-processamento: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Pré-processa dados SINAN')
    parser.add_argument('--sem-filtro-violencia', action='store_true',
                        help='Não filtrar por violência (mantém todos os registros 0-17 anos)')
    args = parser.parse_args()
    
    # Se --sem-filtro-violencia, então filter_violence=False
    filter_violence = not args.sem_filtro_violencia
    
    if not filter_violence:
        print("[AVISO] Modo sem filtro de violencia ativado!")
        print("[INFO] Todos os registros de 0-17 anos serao mantidos, mesmo sem violencia marcada")
        print()
    
    success = preprocess_data(filter_violence=filter_violence)
    sys.exit(0 if success else 1)
