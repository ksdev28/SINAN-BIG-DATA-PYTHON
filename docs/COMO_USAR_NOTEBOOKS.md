# Como Usar os Notebooks Jupyter

## 📦 Instalação

### Opção 1: Instalar tudo de uma vez
```bash
pip install -r requirements.txt
```

### Opção 2: Instalar apenas Jupyter
```bash
pip install jupyter jupyterlab
```

## 🚀 Como Executar

### Método 1: Jupyter Notebook (Tradicional) ✅ FUNCIONA
```bash
# Da raiz do projeto - USE ESTE COMANDO
python -m notebook notebooks/analise_performance_dados.ipynb
```
- Abre automaticamente no navegador
- Interface clássica do Jupyter
- **Nota**: Se `jupyter notebook` não funcionar, use `python -m notebook`

### Método 2: JupyterLab (Recomendado - Mais Moderno)
```bash
# Da raiz do projeto
python -m jupyterlab
```
- Abre interface moderna no navegador
- Navegue até `notebooks/analise_performance_dados.ipynb`
- Melhor para trabalhar com múltiplos arquivos

### Método 3: VS Code (Se você usa VS Code)
1. Abra o VS Code
2. Abra o arquivo `notebooks/analise_performance_dados.ipynb`
3. O VS Code tem suporte nativo para notebooks
4. Não precisa instalar Jupyter separadamente

### Método 4: Executar sem interface (linha de comando)
```bash
jupyter nbconvert --to notebook --execute notebooks/analise_performance_dados.ipynb
```
- Executa o notebook sem abrir interface
- Útil para automação

## 📝 Dicas

1. **Primeira vez**: Execute `jupyter notebook` ou `jupyter lab` na raiz do projeto
2. **Navegação**: Use a interface do Jupyter para navegar entre arquivos
3. **Kernel**: Certifique-se de que o kernel Python está selecionado
4. **Interrupção**: Use `Ctrl+C` no terminal para parar o servidor Jupyter

## 🔧 Solução de Problemas

### "jupyter não é reconhecido" ou "jupyter notebook não funciona"
✅ **SOLUÇÃO**: Use sempre `python -m notebook` em vez de `jupyter notebook`
```bash
# Em vez de:
jupyter notebook notebooks/analise_performance_dados.ipynb

# Use:
python -m notebook notebooks/analise_performance_dados.ipynb
```

### "jupyter lab não funciona"
✅ **SOLUÇÃO**: Use `python -m jupyterlab`
```bash
python -m jupyterlab
```

### Porta já em uso
- Use outra porta: `jupyter notebook --port 8889`

### Kernel não encontrado
- Instale: `pip install ipykernel`
- Registre: `python -m ipykernel install --user`

## ✅ Recomendação

Para este projeto, recomendo usar **JupyterLab**:
```bash
pip install jupyterlab
jupyter lab
```

É mais moderno e oferece melhor experiência para análise de dados!

