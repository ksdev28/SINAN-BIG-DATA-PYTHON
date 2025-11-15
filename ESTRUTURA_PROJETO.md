# Estrutura do Projeto SINAN Big Data Python

## 📁 Organização de Pastas

### `/src` - Código Fonte Principal

Contém todo o código Python do projeto organizado em módulos.

- **`dashboard_sinan_real_data.py`**: Dashboard Streamlit principal
- **`processors/`**: Processadores de dados
  - `sinan_data_processor_comprehensive.py`: Processador completo com Pandas
  - `sinan_data_processor_duckdb.py`: Processador otimizado com DuckDB
- **`utils/`**: Utilitários
  - `munic_dict_loader.py`: Carregador de dicionários de municípios

### `/notebooks` - Jupyter Notebooks

Notebooks para análises exploratórias e documentação interativa.

- `analise_performance_dados.ipynb`: Análise de performance e gargalos

### `/scripts` - Scripts Temporários

Scripts de análise e exploração que podem ser executados independentemente.

- `explore_columns.py`: Exploração de colunas disponíveis
- `analise_status_casos.py`: Análise específica de status de casos

### `/data` - Dados do Projeto

Todos os dados do projeto organizados por tipo.

- **`raw/`**: Dados brutos
  - `VIOLBR-PARQUET/`: Arquivos Parquet com dados SINAN
- **`config/`**: Arquivos de configuração
  - `TAB_SINANONLINE/`: Arquivos .cnv com dicionários SINAN

### `/docs` - Documentação

Toda a documentação do projeto.

- `README_DASHBOARD.md`: Documentação do dashboard
- `README_DUCKDB.md`: Documentação sobre DuckDB
- `ANALISE_TECNICA_COMPLETA.md`: Análise técnica completa
- PDFs e outros documentos

### `/reports` - Relatórios Gerados

Relatórios e saídas de análises.

- `relatorio_colunas_sinan.txt`
- `relatorio_status_casos.txt`

## 🔄 Como Executar

### Dashboard

```bash
# Da raiz do projeto
streamlit run src/dashboard_sinan_real_data.py
```

### Scripts

```bash
# Da raiz do projeto
python scripts/explore_columns.py
python scripts/analise_status_casos.py
```

### Notebooks

```bash
# Da raiz do projeto
jupyter notebook notebooks/analise_performance_dados.ipynb
```

## 📝 Convenções

1. **Imports**: Sempre use imports absolutos a partir de `src/`
2. **Caminhos**: Use `project_root` definido no dashboard para caminhos relativos
3. **Novos Arquivos**: Coloque na pasta apropriada conforme a função
4. **Scripts Temporários**: Use `/scripts` para análises pontuais
5. **Documentação**: Adicione em `/docs` quando criar nova documentação

## 🎯 Benefícios da Estrutura

- ✅ Organização clara e intuitiva
- ✅ Fácil manutenção e navegação
- ✅ Separação de responsabilidades
- ✅ Facilita colaboração em equipe
- ✅ Escalável para crescimento do projeto
