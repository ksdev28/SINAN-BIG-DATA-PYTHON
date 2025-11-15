#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para explorar todas as colunas disponíveis nos dados SINAN
e verificar informações sobre datas, status, prisões, etc.
"""

import pandas as pd
from pathlib import Path

def explore_sinan_columns():
    """Explora todas as colunas disponíveis nos dados SINAN"""
    
    parquet_path = Path("data/raw/VIOLBR-PARQUET")
    parquet_files = list(parquet_path.glob("*.parquet"))
    
    if not parquet_files:
        print("ERRO: Nenhum arquivo parquet encontrado em VIOLBR-PARQUET/")
        return
    
    print(f"Encontrados {len(parquet_files)} arquivos parquet")
    print(f"Analisando: {parquet_files[0].name}\n")
    
    # Carregar primeiro arquivo para análise
    df = pd.read_parquet(parquet_files[0])
    
    print("=" * 80)
    print("ANÁLISE DE COLUNAS DISPONÍVEIS")
    print("=" * 80)
    print(f"\nTotal de colunas: {len(df.columns)}")
    print(f"Total de registros: {len(df):,}")
    
    # Listar todas as colunas
    print("\n" + "=" * 80)
    print("TODAS AS COLUNAS DISPONÍVEIS:")
    print("=" * 80)
    for i, col in enumerate(sorted(df.columns.tolist()), 1):
        print(f"{i:3d}. {col}")
    
    # Procurar colunas relacionadas a datas
    print("\n" + "=" * 80)
    print("COLUNAS RELACIONADAS A DATAS:")
    print("=" * 80)
    date_cols = [col for col in df.columns if 'DT_' in col or 'DATA' in col.upper() or 'DATA' in col]
    if date_cols:
        for col in sorted(date_cols):
            print(f"  - {col}")
            # Mostrar amostra de valores
            sample = df[col].dropna().head(3).tolist()
            print(f"    Amostra: {sample}")
    else:
        print("  Nenhuma coluna de data encontrada (além de DT_NOTIFIC)")
    
    # Procurar colunas relacionadas a status/encerramento
    print("\n" + "=" * 80)
    print("COLUNAS RELACIONADAS A STATUS/ENCERRAMENTO:")
    print("=" * 80)
    status_keywords = ['STATUS', 'SITUACAO', 'ENCERR', 'ABANDON', 'FINAL', 'RESULT', 'EVOLUCAO']
    status_cols = []
    for col in df.columns:
        col_upper = col.upper()
        if any(keyword in col_upper for keyword in status_keywords):
            status_cols.append(col)
    
    if status_cols:
        for col in sorted(status_cols):
            print(f"  - {col}")
            # Mostrar valores únicos
            unique_vals = df[col].value_counts().head(5)
            print(f"    Valores únicos (top 5): {unique_vals.to_dict()}")
    else:
        print("  Nenhuma coluna de status encontrada")
    
    # Procurar colunas relacionadas a prisão
    print("\n" + "=" * 80)
    print("COLUNAS RELACIONADAS A PRISÃO/JUSTIÇA:")
    print("=" * 80)
    prisao_keywords = ['PRISAO', 'PRISÃO', 'JUSTICA', 'JUSTIÇA', 'POLICIA', 'POLÍCIA', 'BO', 'DELEGACIA']
    prisao_cols = []
    for col in df.columns:
        col_upper = col.upper()
        if any(keyword in col_upper for keyword in prisao_keywords):
            prisao_cols.append(col)
    
    if prisao_cols:
        for col in sorted(prisao_cols):
            print(f"  - {col}")
            # Mostrar valores únicos
            unique_vals = df[col].value_counts().head(5)
            print(f"    Valores únicos (top 5): {unique_vals.to_dict()}")
    else:
        print("  Nenhuma coluna de prisão encontrada")
    
    # Procurar colunas relacionadas a ocorrência
    print("\n" + "=" * 80)
    print("COLUNAS RELACIONADAS A OCORRÊNCIA:")
    print("=" * 80)
    ocorr_keywords = ['OCORR', 'OCORRÊNCIA', 'OCORRENCIA', 'ACONTEC', 'ACONTECEU']
    ocorr_cols = []
    for col in df.columns:
        col_upper = col.upper()
        if any(keyword in col_upper for keyword in ocorr_keywords):
            ocorr_cols.append(col)
    
    if ocorr_cols:
        for col in sorted(ocorr_cols):
            print(f"  - {col}")
            # Mostrar amostra de valores
            sample = df[col].dropna().head(3).tolist()
            print(f"    Amostra: {sample}")
    else:
        print("  Nenhuma coluna de ocorrência encontrada (além de LOCAL_OCOR)")
    
    # Verificar colunas que estamos usando vs não usando
    print("\n" + "=" * 80)
    print("COLUNAS QUE ESTAMOS USANDO NO DASHBOARD:")
    print("=" * 80)
    colunas_usadas = [
        'DT_NOTIFIC', 'NU_ANO', 'SG_UF_NOT', 'SG_UF', 'ID_MUNICIP', 'ID_MN_RESI',
        'NU_IDADE_N', 'CS_SEXO', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_SEXU', 'VIOL_INFAN',
        'LOCAL_OCOR', 'AUTOR_SEXO', 'AUTOR_ALCO', 'CS_ESCOL_N', 'CS_RACA',
        'SIT_CONJUG', 'ID_OCUPA_N', 'REDE_SAU', 'REDE_EDUCA'
    ]
    rel_cols = [col for col in df.columns if col.startswith('REL_')]
    colunas_usadas.extend(rel_cols)
    
    print(f"Total de colunas usadas: {len([c for c in colunas_usadas if c in df.columns])}")
    for col in sorted([c for c in colunas_usadas if c in df.columns]):
        print(f"  [OK] {col}")
    
    print("\n" + "=" * 80)
    print("COLUNAS DISPONÍVEIS MAS NÃO USADAS:")
    print("=" * 80)
    colunas_nao_usadas = [col for col in df.columns if col not in colunas_usadas]
    print(f"Total de colunas não usadas: {len(colunas_nao_usadas)}")
    for col in sorted(colunas_nao_usadas):
        print(f"  ? {col}")
    
    # Estatísticas gerais
    print("\n" + "=" * 80)
    print("ESTATÍSTICAS GERAIS:")
    print("=" * 80)
    print(f"Total de colunas: {len(df.columns)}")
    print(f"Colunas usadas: {len([c for c in colunas_usadas if c in df.columns])}")
    print(f"Colunas não usadas: {len(colunas_nao_usadas)}")
    print(f"Percentual de colunas usadas: {len([c for c in colunas_usadas if c in df.columns])/len(df.columns)*100:.1f}%")
    
    # Verificar se há dados faltantes importantes
    print("\n" + "=" * 80)
    print("VERIFICAÇÃO DE DADOS FALTANTES:")
    print("=" * 80)
    if 'DT_NOTIFIC' in df.columns:
        total = len(df)
        notific_validas = df['DT_NOTIFIC'].notna().sum()
        print(f"DT_NOTIFIC: {notific_validas:,} válidas de {total:,} ({notific_validas/total*100:.1f}%)")
    
    # Salvar relatório
    with open("relatorio_colunas_sinan.txt", "w", encoding="utf-8") as f:
        f.write("RELATÓRIO DE COLUNAS DISPONÍVEIS NO SINAN\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total de colunas: {len(df.columns)}\n")
        f.write(f"Total de registros: {len(df):,}\n\n")
        f.write("TODAS AS COLUNAS:\n")
        for col in sorted(df.columns.tolist()):
            f.write(f"  - {col}\n")
    
    print("\nRelatorio salvo em: relatorio_colunas_sinan.txt")

if __name__ == "__main__":
    explore_sinan_columns()

