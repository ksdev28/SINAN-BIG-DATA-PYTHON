# Comandos Jupyter - Guia Rápido

## ⚠️ No Windows, use sempre `python -m jupyter` ao invés de apenas `jupyter`

## 📝 Comandos Corretos

### Jupyter Notebook (Tradicional)
```bash
# Da raiz do projeto
python -m jupyter notebook notebooks/analise_performance_dados.ipynb
```

### JupyterLab (Recomendado)
```bash
# Da raiz do projeto
python -m jupyter lab
```
Depois navegue até `notebooks/analise_performance_dados.ipynb` na interface.

### Abrir apenas a interface (sem especificar arquivo)
```bash
# Jupyter Notebook
python -m jupyter notebook

# JupyterLab
python -m jupyter lab
```

## 🔧 Alternativas se ainda não funcionar

### Opção 1: Usar caminho absoluto
```bash
python -m jupyter notebook "C:\Users\kelve\OneDrive\Documentos\SINAN-BIG-DATA-PYTHON\notebooks\analise_performance_dados.ipynb"
```

### Opção 2: Abrir JupyterLab e navegar
```bash
python -m jupyter lab
```
Depois use a interface para abrir o arquivo.

### Opção 3: VS Code
- Abra o VS Code
- Abra o arquivo `notebooks/analise_performance_dados.ipynb`
- Funciona nativamente sem precisar do Jupyter instalado

## ✅ Comando Recomendado

Para este projeto, use:
```bash
python -m jupyter lab
```

É mais moderno e funciona melhor no Windows!

