#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para executar o Dashboard SINAN
Pode ser executado diretamente: python run_dashboard.py
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Executa o dashboard Streamlit"""
    print("=" * 50)
    print("  Dashboard SINAN - Violência Infantil")
    print("=" * 50)
    print()
    print("Iniciando o dashboard...")
    print()
    
    # Caminho do dashboard
    dashboard_path = Path(__file__).parent / "src" / "dashboard_sinan_real_data.py"
    
    if not dashboard_path.exists():
        print(f"ERRO: Arquivo não encontrado: {dashboard_path}")
        sys.exit(1)
    
    try:
        # Executar streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run",
            str(dashboard_path)
        ])
    except KeyboardInterrupt:
        print("\n\nDashboard encerrado pelo usuário.")
    except Exception as e:
        print(f"\nERRO ao executar dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

