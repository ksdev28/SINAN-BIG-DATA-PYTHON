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
from src.processors.sinan_data_processor_comprehensive import SINANDataProcessorComprehensive
from src.utils.munic_dict_loader import load_municipality_dict

def preprocess_data():
    """Pré-processa os dados SINAN e salva em formato otimizado"""
    
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
        
        # Aplicar dicionários
        print("[INFO] Aplicando dicionarios aos dados...")
        decoded_data = processor.apply_dictionaries(violence_data)
        print("[OK] Dicionarios aplicados")
        print()
        
        # Filtrar apenas crianças e adolescentes (0-17 anos)
        print("[INFO] Filtrando dados para faixa etaria 0-17 anos...")
        if 'NU_IDADE_N' in decoded_data.columns:
            # Filtrar idades válidas (0-17 anos)
            mask_idade = (
                (decoded_data['NU_IDADE_N'] >= 0) & 
                (decoded_data['NU_IDADE_N'] < 18)
            ) | (
                decoded_data['NU_IDADE_N'].isna()  # Manter registros sem idade para análise
            )
            decoded_data = decoded_data[mask_idade].copy()
            print(f"[OK] {len(decoded_data):,} registros apos filtro de idade")
        print()
        
        # Criar colunas derivadas
        print("[INFO] Criando colunas derivadas...")
        
        # Faixa etária
        if 'NU_IDADE_N' in decoded_data.columns:
            def get_age_group(idade):
                if pd.isna(idade):
                    return 'Não informado'
                idade_int = int(idade) if isinstance(idade, (int, float)) else 0
                if idade_int < 2:
                    return '0-1 anos'
                elif idade_int < 6:
                    return '2-5 anos'
                elif idade_int < 10:
                    return '6-9 anos'
                elif idade_int < 14:
                    return '10-13 anos'
                elif idade_int < 18:
                    return '14-17 anos'
                else:
                    return '18+ anos'
            
            decoded_data['FAIXA_ETARIA'] = decoded_data['NU_IDADE_N'].apply(get_age_group)
        
        # Tipo de violência (já deve estar aplicado pelo processador)
        if 'TIPO_VIOLENCIA' not in decoded_data.columns:
            # Se não existir, criar baseado nas colunas REL_
            rel_cols = [col for col in decoded_data.columns if col.startswith('REL_')]
            if rel_cols:
                def get_violence_type(row):
                    tipos = []
                    for col in rel_cols:
                        if pd.notna(row.get(col)) and str(row.get(col)).strip() in ['1', 1, 'Sim']:
                            tipo = col.replace('REL_', '').replace('_', ' ').title()
                            tipos.append(tipo)
                    return ', '.join(tipos) if tipos else 'Não informado'
                
                decoded_data['TIPO_VIOLENCIA'] = decoded_data.apply(get_violence_type, axis=1)
        
        # Processar datas
        date_columns = ['DT_NOTIFIC', 'DT_OCOR', 'DT_ENCERRA']
        for col in date_columns:
            if col in decoded_data.columns:
                try:
                    decoded_data[col] = pd.to_datetime(
                        decoded_data[col], 
                        format='%Y%m%d', 
                        errors='coerce'
                    )
                except:
                    pass
        
        # Processar tempo entre ocorrência e denúncia
        if 'DT_OCOR' in decoded_data.columns and 'DT_NOTIFIC' in decoded_data.columns:
            mask_validas = decoded_data['DT_NOTIFIC'].notna() & decoded_data['DT_OCOR'].notna()
            if mask_validas.any():
                decoded_data.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = (
                    decoded_data.loc[mask_validas, 'DT_NOTIFIC'] - 
                    decoded_data.loc[mask_validas, 'DT_OCOR']
                ).dt.days
                # Filtrar valores inválidos
                decoded_data.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = decoded_data.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'].apply(
                    lambda x: x if pd.notna(x) and 0 <= x <= 3650 else None
                )
        
        # Processar encaminhamentos para justiça
        encaminhamento_cols = ['ENC_DELEG', 'ENC_DPCA', 'ENC_MPU', 'ENC_VARA']
        if any(col in decoded_data.columns for col in encaminhamento_cols):
            def get_encaminhamentos_justica(row):
                encaminhamentos = []
                for col in encaminhamento_cols:
                    if col in row.index:
                        val = row.get(col)
                        if pd.notna(val) and (str(val).strip() == '1' or val == 1):
                            if col == 'ENC_DELEG':
                                encaminhamentos.append('Delegacia')
                            elif col == 'ENC_DPCA':
                                encaminhamentos.append('DPCA')
                            elif col == 'ENC_MPU':
                                encaminhamentos.append('Ministério Público')
                            elif col == 'ENC_VARA':
                                encaminhamentos.append('Vara da Infância')
                return ', '.join(encaminhamentos) if encaminhamentos else 'Nenhum'
            
            decoded_data['ENCAMINHAMENTOS_JUSTICA'] = decoded_data.apply(get_encaminhamentos_justica, axis=1)
        else:
            decoded_data['ENCAMINHAMENTOS_JUSTICA'] = 'Não informado'
        
        print("[OK] Colunas derivadas criadas")
        print()
        
        # Otimizar tipos de dados para economizar memória
        print("[INFO] Otimizando tipos de dados...")
        
        # Converter colunas object com poucos valores únicos para category
        for col in decoded_data.select_dtypes(include=['object']).columns:
            if decoded_data[col].nunique() < len(decoded_data) * 0.5:  # Menos de 50% valores únicos
                try:
                    decoded_data[col] = decoded_data[col].astype('category')
                except:
                    pass
        
        # Converter int64 para int32 quando possível
        for col in decoded_data.select_dtypes(include=['int64']).columns:
            try:
                if decoded_data[col].min() >= -2147483648 and decoded_data[col].max() <= 2147483647:
                    decoded_data[col] = decoded_data[col].astype('int32')
            except:
                pass
        
        print("[OK] Tipos de dados otimizados")
        print()
        
        # Salvar dados processados
        output_file = processed_dir / "sinan_data_processed.parquet"
        print(f"[INFO] Salvando dados processados em {output_file}...")
        
        decoded_data.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',  # Compressão rápida
            index=False
        )
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"[OK] Dados salvos! Tamanho: {file_size_mb:.2f} MB")
        print()
        
        # Salvar metadados
        metadata = {
            'total_registros': len(decoded_data),
            'colunas': list(decoded_data.columns),
            'data_processamento': pd.Timestamp.now().isoformat(),
            'versao': '1.0'
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
        print(f"Total de registros: {len(decoded_data):,}")
        print(f"Total de colunas: {len(decoded_data.columns)}")
        print(f"Tamanho do arquivo: {file_size_mb:.2f} MB")
        print()
        
        if 'FAIXA_ETARIA' in decoded_data.columns:
            print("Distribuicao por faixa etaria:")
            print(decoded_data['FAIXA_ETARIA'].value_counts().head(10))
            print()
        
        if 'SEXO' in decoded_data.columns:
            print("Distribuicao por sexo:")
            print(decoded_data['SEXO'].value_counts())
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
    success = preprocess_data()
    sys.exit(0 if success else 1)

