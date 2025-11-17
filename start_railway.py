#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de inicialização para Railway
Roda o preprocessamento e inicia o dashboard Streamlit
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Inicializa o dashboard no Railway"""
    print("=" * 70)
    print("INICIALIZANDO DASHBOARD SINAN NO RAILWAY")
    print("=" * 70)
    print()
    
    # Verificar se os dados pré-processados existem
    processed_file = Path("data/processed/sinan_data_processed.parquet")
    
    if not processed_file.exists():
        print("[AVISO] Dados pré-processados não encontrados.")
        print("[AVISO] O preprocessamento requer muita memória (~4GB+).")
        print("[AVISO] Recomendado: Faça commit dos dados processados no Git.")
        print()
        print("[INFO] Tentando executar preprocessamento...")
        print("[INFO] Isso pode falhar por falta de memória no Railway.")
        print()
        
        # Verificar se os dados brutos existem
        raw_data_dir = Path("data/raw/VIOLBR-PARQUET")
        if not raw_data_dir.exists() or not list(raw_data_dir.glob("*.parquet")):
            print("[ERRO] Dados brutos não encontrados em data/raw/VIOLBR-PARQUET/")
            print("[ERRO] Não é possível fazer preprocessamento sem dados brutos.")
            print("[ERRO] Solução: Faça commit dos dados processados no Git.")
            sys.exit(1)
        
        try:
            # Executar preprocessamento
            # Garantir que estamos no diretório correto
            script_path = Path("scripts/preprocess_data.py")
            if not script_path.exists():
                # Tentar caminho absoluto
                script_path = Path(__file__).parent / "scripts" / "preprocess_data.py"
            
            result = subprocess.run(
                [sys.executable, str(script_path), "--sem-filtro-violencia"],
                check=True,
                capture_output=False,
                cwd=Path(__file__).parent
            )
            
            if result.returncode == 0:
                print()
                print("[OK] Preprocessamento concluído com sucesso!")
            else:
                print("[ERRO] Falha no preprocessamento")
                sys.exit(1)
                
        except subprocess.CalledProcessError as e:
            print(f"[ERRO] Erro ao executar preprocessamento: {e}")
            print("[ERRO] Provavelmente falta de memória.")
            print("[SOLUÇÃO] Faça commit dos dados processados no Git e faça deploy novamente.")
            sys.exit(1)
        except FileNotFoundError:
            print("[ERRO] Script de preprocessamento não encontrado")
            sys.exit(1)
    else:
        print("[OK] Dados pré-processados encontrados!")
        print("[OK] Usando dados do Git. Preprocessamento não necessário.")
        print()
    
    # Obter porta do Railway (ou usar padrão 8501)
    port = os.environ.get("PORT", "8501")
    
    print("[INFO] Iniciando dashboard Streamlit...")
    print(f"[INFO] Porta: {port}")
    print()
    
    # Caminho do dashboard
    dashboard_path = Path("src/dashboard_sinan_real_data.py")
    
    if not dashboard_path.exists():
        print(f"[ERRO] Dashboard não encontrado: {dashboard_path}")
        sys.exit(1)
    
    try:
        # Executar Streamlit
        # Railway expõe a porta via variável de ambiente PORT
        # Usar os.environ para passar variáveis de ambiente ao Streamlit
        env = os.environ.copy()
        
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(dashboard_path),
            "--server.port", str(port),
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ], env=env)
    except KeyboardInterrupt:
        print("\n\nDashboard encerrado.")
    except Exception as e:
        print(f"\n[ERRO] Erro ao executar dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

