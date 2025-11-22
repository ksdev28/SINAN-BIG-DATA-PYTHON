#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico de Performance - Dashboard SINAN
Identifica gargalos e problemas de desempenho
"""

import pandas as pd
import numpy as np
import time
import psutil
import os
from pathlib import Path
import sys

def get_memory_usage():
    """Retorna o uso de memória atual em MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def format_bytes(bytes_size):
    """Formata bytes para formato legível"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

print("=" * 80)
print("DIAGNÓSTICO DE PERFORMANCE - DASHBOARD SINAN")
print("=" * 80)

# 1. Memória inicial
memoria_inicial = get_memory_usage()
print(f"\n📊 Memória inicial: {memoria_inicial:.2f} MB")

# 2. Encontrar arquivos Parquet
project_root = Path(__file__).parent
parquet_dir = project_root / "data" / "raw" / "VIOLBR-PARQUET"
arquivos_parquet = list(parquet_dir.glob('*.parquet'))

print(f"\n📁 Arquivos Parquet encontrados: {len(arquivos_parquet)}")
for arquivo in arquivos_parquet:
    tamanho = arquivo.stat().st_size
    print(f"  - {arquivo.name}: {format_bytes(tamanho)}")

# 3. Carregar dados processados (se existir)
processed_file = project_root / "data" / "processed" / "sinan_data_processed.parquet"
if processed_file.exists():
    print(f"\n✅ Carregando dados pré-processados: {processed_file.name}")
    inicio = time.time()
    mem_antes = get_memory_usage()
    
    try:
        df = pd.read_parquet(processed_file)
        
        fim = time.time()
        mem_depois = get_memory_usage()
        
        tempo_carregamento = fim - inicio
        memoria_usada = mem_depois - mem_antes
        
        print(f"\n=== RESULTADOS DO CARREGAMENTO ===")
        print(f"⏱️  Tempo de carregamento: {tempo_carregamento:.2f} segundos")
        print(f"💾 Memória usada: {memoria_usada:.2f} MB")
        print(f"💾 Memória total após carregamento: {mem_depois:.2f} MB")
        print(f"\n📊 Shape do DataFrame: {df.shape}")
        print(f"📊 Total de registros: {len(df):,}")
        print(f"📊 Total de colunas: {len(df.columns)}")
        
        # 4. Teste específico do gargalo - Expandir tipos de violência
        print("\n" + "=" * 80)
        print("🔴 TESTE DO GARGALO IDENTIFICADO: Expandir Tipos de Violência")
        print("=" * 80)
        
        if 'TIPO_VIOLENCIA' in df.columns:
            # Verificar quantos registros têm tipos combinados
            tipos_combinados = df['TIPO_VIOLENCIA'].astype(str).str.contains(',', na=False).sum()
            print(f"\n📈 Registros com tipos combinados: {tipos_combinados:,} ({tipos_combinados/len(df)*100:.1f}%)")
            
            # Método RÁPIDO (vetorizado) - O que estamos usando agora
            print("\n✅ Testando método RÁPIDO (vetorizado - atual):")
            inicio = time.time()
            mem_antes = get_memory_usage()
            
            try:
                df_temp = df[['TIPO_VIOLENCIA']].copy()
                df_temp = df_temp[df_temp['TIPO_VIOLENCIA'].notna()]
                df_temp['TIPO_VIOLENCIA'] = df_temp['TIPO_VIOLENCIA'].astype(str)
                df_temp = df_temp[~df_temp['TIPO_VIOLENCIA'].isin(['nan', 'None', '', 'Não especificado'])]
                
                # Verificar tamanho antes do explode
                print(f"   Registros válidos antes do explode: {len(df_temp):,}")
                
                df_temp['TIPO_VIOLENCIA'] = df_temp['TIPO_VIOLENCIA'].str.split(',')
                df_tipos_expandidos = df_temp.explode('TIPO_VIOLENCIA')
                
                mem_depois = get_memory_usage()
                memoria_explode = mem_depois - mem_antes
                
                df_tipos_expandidos['TIPO_VIOLENCIA'] = df_tipos_expandidos['TIPO_VIOLENCIA'].str.strip()
                df_tipos_expandidos = df_tipos_expandidos[
                    df_tipos_expandidos['TIPO_VIOLENCIA'].isin(['Sexual', 'Física', 'Psicológica'])
                ]
                
                tempo_rapido = time.time() - inicio
                print(f"   ⏱️  Tempo total: {tempo_rapido:.4f}s")
                print(f"   💾 Memória adicional usada: {memoria_explode:.2f} MB")
                print(f"   📊 Registros após expandir: {len(df_tipos_expandidos):,}")
                print(f"   ✅ SUCESSO!")
                
            except Exception as e:
                print(f"   ❌ ERRO: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print("   ⚠️  Coluna TIPO_VIOLENCIA não encontrada")
            print("   Tentando criar a partir das colunas VIOL_*...")
            
            # Tentar criar TIPO_VIOLENCIA
            violencia_cols = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO']
            cols_disponiveis = [col for col in violencia_cols if col in df.columns]
            
            if cols_disponiveis:
                print(f"   Colunas de violência encontradas: {cols_disponiveis}")
                print("   Criando TIPO_VIOLENCIA...")
                
                inicio = time.time()
                tipos_list = []
                for _, row in df.head(1000).iterrows():  # Teste com 1000 linhas
                    tipos = []
                    if str(row.get('VIOL_FISIC', '')).upper() in ['1', 'SIM', 'S', '1.0']:
                        tipos.append('Física')
                    if str(row.get('VIOL_PSICO', '')).upper() in ['1', 'SIM', 'S', '1.0']:
                        tipos.append('Psicológica')
                    if str(row.get('VIOL_SEXU', '')).upper() in ['1', 'SIM', 'S', '1.0']:
                        tipos.append('Sexual')
                    tipos_list.append(', '.join(tipos) if tipos else 'Não especificado')
                
                tempo_criacao = time.time() - inicio
                print(f"   ⏱️  Tempo para criar TIPO_VIOLENCIA (1000 linhas): {tempo_criacao:.4f}s")
                print(f"   ⚠️  Estimativa para {len(df):,} linhas: {(tempo_criacao * len(df) / 1000):.2f}s")
                print(f"   ⚠️  Este é um GARGALO! Usar método vetorizado.")
        
        # 5. Análise de memória
        print("\n" + "=" * 80)
        print("💾 ANÁLISE DE MEMÓRIA")
        print("=" * 80)
        
        memoria_atual = get_memory_usage()
        memoria_df = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        
        print(f"\nMemória total do processo: {memoria_atual:.2f} MB")
        print(f"Memória usada pelo DataFrame: {memoria_df:.2f} MB")
        print(f"Memória adicional (overhead): {memoria_atual - memoria_df:.2f} MB")
        
        # Top 10 colunas que mais consomem memória
        print("\n📊 Top 10 colunas que mais consomem memória:")
        memoria_por_coluna = df.memory_usage(deep=True) / 1024 / 1024
        top_colunas = memoria_por_coluna.nlargest(10)
        for col, mem in top_colunas.items():
            pct = (mem / memoria_df * 100) if memoria_df > 0 else 0
            print(f"   {col}: {mem:.2f} MB ({pct:.1f}%)")
        
        # 6. Recomendações
        print("\n" + "=" * 80)
        print("💡 RECOMENDAÇÕES DE OTIMIZAÇÃO")
        print("=" * 80)
        
        recomendacoes = []
        
        # Verificar colunas object que poderiam ser category
        colunas_object = df.select_dtypes(include=['object']).columns
        colunas_candidatas_category = []
        for col in colunas_object[:20]:  # Limitar para não demorar muito
            try:
                if df[col].nunique() < len(df) * 0.1:  # Menos de 10% de valores únicos
                    colunas_candidatas_category.append(col)
            except:
                pass
        
        if colunas_candidatas_category:
            recomendacoes.append(f"✅ Converter {len(colunas_candidatas_category)} colunas object para category")
            print(f"\n✅ Converter colunas object para category:")
            for col in colunas_candidatas_category[:5]:
                print(f"   - {col}")
        
        # Verificar se explode está sendo usado corretamente
        if 'TIPO_VIOLENCIA' in df.columns:
            tipos_unicos = df['TIPO_VIOLENCIA'].nunique()
            print(f"\n📊 Tipos únicos de violência: {tipos_unicos}")
            if tipos_unicos > 10:
                print("   ⚠️  Muitos tipos únicos - considere filtrar antes do explode")
        
        print("\n✅ DIAGNÓSTICO CONCLUÍDO!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERRO ao carregar dados: {str(e)}")
        import traceback
        traceback.print_exc()
else:
    print(f"\n⚠️  Arquivo pré-processado não encontrado: {processed_file}")
    print("   Execute o preprocessamento primeiro ou carregue dados raw")

