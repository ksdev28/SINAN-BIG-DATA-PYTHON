#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de teste para verificar o carregamento dos dados pré-processados
"""

import pandas as pd
import time
from pathlib import Path

project_root = Path(__file__).parent
processed_file = project_root / "data" / "processed" / "sinan_data_processed.parquet"

print("=" * 80)
print("TESTE DE CARREGAMENTO DE DADOS PRÉ-PROCESSADOS")
print("=" * 80)

if not processed_file.exists():
    print(f"❌ Arquivo não encontrado: {processed_file}")
    exit(1)

print(f"✅ Arquivo encontrado: {processed_file}")
print(f"📊 Tamanho do arquivo: {processed_file.stat().st_size / (1024*1024):.2f} MB")

print("\n⏱️  Carregando dados...")
inicio = time.time()

try:
    df = pd.read_parquet(processed_file, engine='pyarrow')
    tempo_carregamento = time.time() - inicio
    
    print(f"✅ Dados carregados com sucesso!")
    print(f"⏱️  Tempo de carregamento: {tempo_carregamento:.2f} segundos")
    print(f"📊 Total de registros: {len(df):,}")
    print(f"📊 Total de colunas: {len(df.columns)}")
    
    # Verificar colunas derivadas
    colunas_derivadas = ['ANO_NOTIFIC', 'TIPO_VIOLENCIA', 'SEXO', 'UF_NOTIFIC', 'FAIXA_ETARIA']
    colunas_faltando = [col for col in colunas_derivadas if col not in df.columns]
    
    if colunas_faltando:
        print(f"\n⚠️  Colunas derivadas faltando: {colunas_faltando}")
    else:
        print(f"\n✅ Todas as colunas derivadas existem")
    
    # Verificar memória
    memoria_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    print(f"💾 Memória usada: {memoria_mb:.2f} MB")
    
    print("\n✅ TESTE CONCLUÍDO COM SUCESSO!")
    
except Exception as e:
    print(f"\n❌ ERRO ao carregar dados: {str(e)}")
    import traceback
    traceback.print_exc()
    exit(1)

