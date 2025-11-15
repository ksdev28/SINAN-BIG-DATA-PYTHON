#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para limpar o cache do Streamlit manualmente
Execute: python limpar_cache_streamlit.py
"""

import os
import shutil
from pathlib import Path

def limpar_cache_streamlit():
    """Limpa o cache do Streamlit"""
    # Caminhos comuns do cache do Streamlit no Windows
    cache_paths = [
        Path.home() / ".streamlit" / "cache",
        Path(os.getenv("APPDATA", "")) / "streamlit" / "cache",
        Path(os.getenv("LOCALAPPDATA", "")) / "streamlit" / "cache",
    ]
    
    print("Limpando cache do Streamlit...")
    cache_encontrado = False
    
    for cache_path in cache_paths:
        if cache_path.exists():
            try:
                shutil.rmtree(cache_path)
                print(f"[OK] Cache removido: {cache_path}")
                cache_encontrado = True
            except Exception as e:
                print(f"[ERRO] Erro ao remover {cache_path}: {e}")
    
    if not cache_encontrado:
        print("[INFO] Nenhum cache encontrado nos locais padrao.")
        print("\nPara limpar manualmente:")
        print("1. Feche o Streamlit se estiver rodando")
        print("2. Execute: python -m streamlit cache clear")
        print("3. Ou delete a pasta .streamlit/cache manualmente")
    else:
        print("\n[OK] Cache limpo com sucesso!")
        print("Agora você pode executar o dashboard novamente.")

if __name__ == "__main__":
    limpar_cache_streamlit()

