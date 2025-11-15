#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carregador de Dicionários de Municípios do SINAN
Lê arquivos .cnv e cria dicionário de códigos para nomes
"""

from pathlib import Path
import re

def load_municipality_dict(cnv_path="data/config/TAB_SINANONLINE"):
    """
    Carrega dicionário de municípios de todos os arquivos .cnv
    Retorna dict: {codigo: nome_municipio}
    """
    cnv_dir = Path(cnv_path)
    municip_dict = {}
    
    # Padrão para arquivos de municípios
    munic_files = list(cnv_dir.glob("Munic*.cnv")) + list(cnv_dir.glob("munic*.cnv"))
    
    for file_path in munic_files:
        try:
            with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                lines = f.readlines()
                
            for line in lines:
                # Ignorar linhas de comentário ou cabeçalho
                if line.strip().startswith(';') or not line.strip():
                    continue
                
                # Padrão: número código nome_município código
                # Exemplo: "1  110001 Alta Floresta D'Oeste  110001"
                # Ou: "1  210005 Açailândia  210005"
                parts = line.strip().split()
                if len(parts) >= 3:
                    try:
                        # O código geralmente está na segunda posição (6 dígitos)
                        codigo = None
                        nome_parts = []
                        
                        # Procurar código de 6 dígitos
                        for i, part in enumerate(parts):
                            if part.isdigit() and len(part) == 6:
                                if codigo is None:
                                    codigo = part
                                    # Nome está depois do código
                                    nome_parts = parts[i+1:]
                                    # Remover último elemento se for código repetido
                                    if len(nome_parts) > 0 and nome_parts[-1].isdigit() and len(nome_parts[-1]) == 6:
                                        nome_parts = nome_parts[:-1]
                                break
                        
                        if codigo and len(codigo) == 6 and nome_parts:
                            nome = ' '.join(nome_parts).strip()
                            
                            if nome and nome.lower() not in ['municipio ignorado', 'ignorado', 'município ignorado']:
                                municip_dict[codigo] = nome
                    except Exception as e:
                        continue
        except Exception as e:
            print(f"Erro ao processar {file_path.name}: {e}")
            continue
    
    return municip_dict

if __name__ == "__main__":
    dict_munic = load_municipality_dict()
    print(f"Total de municípios carregados: {len(dict_munic)}")
    print(f"\nExemplos:")
    for i, (cod, nome) in enumerate(list(dict_munic.items())[:10]):
        print(f"  {cod}: {nome}")

