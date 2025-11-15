#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para analisar a coluna EVOLUCAO e verificar o processamento de status dos casos
"""

import pandas as pd
from pathlib import Path
import sys

def analisar_status_casos():
    """Analisa a coluna EVOLUCAO e verifica o processamento de status"""
    
    print("=" * 80)
    print("ANALISE DE STATUS DE CASOS (EVOLUCAO)")
    print("=" * 80)
    
    # Carregar dados
    parquet_path = Path("data/raw/VIOLBR-PARQUET")
    parquet_files = list(parquet_path.glob("*.parquet"))
    
    if not parquet_files:
        print("ERRO: Nenhum arquivo parquet encontrado em VIOLBR-PARQUET/")
        return
    
    print(f"\nEncontrados {len(parquet_files)} arquivos parquet")
    print(f"Analisando: {parquet_files[0].name}\n")
    
    # Carregar primeiro arquivo
    df = pd.read_parquet(parquet_files[0])
    
    print(f"Total de registros: {len(df):,}")
    
    # Verificar se coluna EVOLUCAO existe
    if 'EVOLUCAO' not in df.columns:
        print("\nERRO: Coluna EVOLUCAO nao encontrada!")
        print(f"Colunas disponiveis: {sorted(df.columns.tolist())}")
        return
    
    print(f"\nColuna EVOLUCAO encontrada!")
    
    # Analisar valores únicos
    print("\n" + "=" * 80)
    print("VALORES UNICOS NA COLUNA EVOLUCAO:")
    print("=" * 80)
    
    valores_unicos = df['EVOLUCAO'].value_counts()
    print(f"\nTotal de valores unicos: {len(valores_unicos)}")
    print(f"\nTop 20 valores mais frequentes:")
    for valor, count in valores_unicos.head(20).items():
        percentual = (count / len(df) * 100)
        print(f"  '{valor}': {count:,} ({percentual:.2f}%)")
    
    # Analisar valores nulos/vazios
    print("\n" + "=" * 80)
    print("ANALISE DE VALORES NULOS/VAZIOS:")
    print("=" * 80)
    
    nulos = df['EVOLUCAO'].isna().sum()
    vazios = (df['EVOLUCAO'] == '').sum() if 'EVOLUCAO' in df.columns else 0
    espacos = (df['EVOLUCAO'].astype(str).str.strip() == '').sum()
    
    print(f"Valores nulos (NaN): {nulos:,} ({(nulos/len(df)*100):.2f}%)")
    print(f"Valores vazios ('): {vazios:,} ({(vazios/len(df)*100):.2f}%)")
    print(f"Valores apenas espacos: {espacos:,} ({(espacos/len(df)*100):.2f}%)")
    
    # Analisar tipos de dados
    print("\n" + "=" * 80)
    print("TIPOS DE DADOS:")
    print("=" * 80)
    print(f"Tipo da coluna: {df['EVOLUCAO'].dtype}")
    print(f"Exemplo de valores (primeiros 10):")
    for i, val in enumerate(df['EVOLUCAO'].head(10)):
        print(f"  [{i}] {repr(val)} (tipo: {type(val).__name__})")
    
    # Testar mapeamento atual
    print("\n" + "=" * 80)
    print("TESTE DO MAPEAMENTO ATUAL:")
    print("=" * 80)
    
    def map_evolucao_atual(val):
        """Mapeamento atual do código"""
        if pd.isna(val) or val == '':
            return 'Não informado'
        val_str = str(val).strip().upper()
        # Mapear códigos de evolução (exemplos comuns)
        if val_str in ['Y04', 'Y04.0', '4']:
            return 'Encerrado'
        elif val_str in ['Y08', 'Y08.0', '8']:
            return 'Abandonado'
        elif val_str in ['Y09', 'Y09.0', '9']:
            return 'Ignorado'
        else:
            return f'Outro ({val_str})'
    
    # Aplicar mapeamento
    df['STATUS_TESTE'] = df['EVOLUCAO'].apply(map_evolucao_atual)
    
    print("\nResultado do mapeamento:")
    status_counts = df['STATUS_TESTE'].value_counts()
    for status, count in status_counts.items():
        percentual = (count / len(df) * 100)
        print(f"  {status}: {count:,} ({percentual:.2f}%)")
    
    # Analisar valores que não foram mapeados corretamente
    print("\n" + "=" * 80)
    print("VALORES NAO MAPEADOS (Outro):")
    print("=" * 80)
    
    outros = df[df['STATUS_TESTE'].str.startswith('Outro')]
    if len(outros) > 0:
        print(f"Total de registros nao mapeados: {len(outros):,}")
        print("\nValores unicos nao mapeados:")
        valores_nao_mapeados = outros['EVOLUCAO'].value_counts().head(20)
        for valor, count in valores_nao_mapeados.items():
            print(f"  '{valor}': {count:,}")
    
    # Verificar se há padrões nos valores
    print("\n" + "=" * 80)
    print("ANALISE DE PADROES:")
    print("=" * 80)
    
    # Verificar se valores são numéricos
    valores_numericos = df['EVOLUCAO'].apply(lambda x: str(x).isdigit() if pd.notna(x) else False).sum()
    print(f"Valores que sao apenas digitos: {valores_numericos:,}")
    
    # Verificar se valores começam com Y
    valores_y = df['EVOLUCAO'].apply(lambda x: str(x).startswith('Y') if pd.notna(x) else False).sum()
    print(f"Valores que comecam com 'Y': {valores_y:,}")
    
    # Verificar se valores contêm ponto
    valores_ponto = df['EVOLUCAO'].apply(lambda x: '.' in str(x) if pd.notna(x) else False).sum()
    print(f"Valores que contem ponto: {valores_ponto:,}")
    
    # Verificar comprimento dos valores
    print("\nComprimento dos valores (primeiros 20):")
    for i, val in enumerate(df['EVOLUCAO'].head(20)):
        if pd.notna(val):
            print(f"  '{val}': comprimento = {len(str(val))}")
    
    # Sugerir mapeamento melhorado
    print("\n" + "=" * 80)
    print("SUGESTAO DE MAPEAMENTO MELHORADO:")
    print("=" * 80)
    
    # Analisar todos os valores únicos e sugerir mapeamento
    valores_unicos_list = df['EVOLUCAO'].dropna().unique()
    
    print("\nMapeamento sugerido baseado nos valores encontrados:")
    mapeamento_sugerido = {}
    
    for val in valores_unicos_list[:50]:  # Primeiros 50 valores únicos
        val_str = str(val).strip().upper()
        
        # Padrões comuns
        if val_str == '' or val_str == 'NAN' or pd.isna(val):
            mapeamento_sugerido[val] = 'Não informado'
        elif val_str in ['Y04', 'Y04.0', '4', '04']:
            mapeamento_sugerido[val] = 'Encerrado'
        elif val_str in ['Y08', 'Y08.0', '8', '08']:
            mapeamento_sugerido[val] = 'Abandonado'
        elif val_str in ['Y09', 'Y09.0', '9', '09']:
            mapeamento_sugerido[val] = 'Ignorado'
        elif val_str.startswith('Y04'):
            mapeamento_sugerido[val] = 'Encerrado'
        elif val_str.startswith('Y08'):
            mapeamento_sugerido[val] = 'Abandonado'
        elif val_str.startswith('Y09'):
            mapeamento_sugerido[val] = 'Ignorado'
        elif val_str.isdigit():
            num = int(val_str)
            if num == 4:
                mapeamento_sugerido[val] = 'Encerrado'
            elif num == 8:
                mapeamento_sugerido[val] = 'Abandonado'
            elif num == 9:
                mapeamento_sugerido[val] = 'Ignorado'
            else:
                mapeamento_sugerido[val] = f'Outro (codigo {num})'
        else:
            mapeamento_sugerido[val] = f'Outro ({val_str})'
    
    print("\nPrimeiros 30 mapeamentos sugeridos:")
    for i, (val, status) in enumerate(list(mapeamento_sugerido.items())[:30]):
        print(f"  '{val}' -> '{status}'")
    
    # Salvar relatório
    with open("relatorio_status_casos.txt", "w", encoding="utf-8") as f:
        f.write("RELATORIO DE ANALISE DE STATUS DE CASOS\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total de registros: {len(df):,}\n\n")
        f.write("VALORES UNICOS:\n")
        for valor, count in valores_unicos.head(50).items():
            f.write(f"  '{valor}': {count:,}\n")
        f.write("\nMAPEAMENTO SUGERIDO:\n")
        for val, status in list(mapeamento_sugerido.items())[:50]:
            f.write(f"  '{val}' -> '{status}'\n")
    
    print("\nRelatorio salvo em: relatorio_status_casos.txt")
    print("\n" + "=" * 80)
    print("ANALISE CONCLUIDA")
    print("=" * 80)

if __name__ == "__main__":
    analisar_status_casos()

