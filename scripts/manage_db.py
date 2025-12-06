import os
from pathlib import Path
import sys

# Tamanho do pedaço em bytes (50MB)
CHUNK_SIZE = 50 * 1024 * 1024 

def split_database():
    """Divide o sinan.duckdb em partes menores para o Git"""
    project_root = Path(__file__).parent.parent
    db_path = project_root / "data/processed/sinan.duckdb"
    
    if not db_path.exists():
        print(f"Erro: Arquivo {db_path} não encontrado.")
        return

    print(f"Dividindo {db_path.name} ({db_path.stat().st_size / 1024 / 1024:.2f} MB)...")
    
    part_num = 0
    with open(db_path, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            
            part_name = f"{db_path.name}.part{part_num:03d}"
            part_path = db_path.parent / part_name
            
            with open(part_path, 'wb') as part_file:
                part_file.write(chunk)
            
            print(f"  Criado {part_name}")
            part_num += 1
            
    print("Divisão concluída!")

def join_database():
    """Reconstroi o sinan.duckdb a partir das partes"""
    project_root = Path(__file__).parent.parent
    processed_dir = project_root / "data/processed"
    output_path = processed_dir / "sinan.duckdb"
    
    # Encontrar partes
    parts = sorted([p for p in processed_dir.glob("sinan.duckdb.part*")])
    
    if not parts:
        print("Nenhuma parte encontrada para remontar.")
        return False

    print(f"Reconstruindo banco de dados a partir de {len(parts)} partes...")
    
    with open(output_path, 'wb') as outfile:
        for part in parts:
            print(f"  Lendo {part.name}...")
            with open(part, 'rb') as infile:
                outfile.write(infile.read())
                
    print(f"Reconstrução concluída: {output_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "join":
        join_database()
    else:
        split_database()
