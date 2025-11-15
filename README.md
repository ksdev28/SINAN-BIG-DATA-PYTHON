# SINAN Big Data Python

Sistema de análise de dados do SINAN (Sistema de Informação de Agravos de Notificação) para análise de violência contra crianças e adolescentes.

## 📁 Estrutura do Projeto

```
SINAN-BIG-DATA-PYTHON/
├── README.md                    # Este arquivo
├── requirements.txt             # Dependências do projeto
├── pyrightconfig.json           # Configuração do linter
│
├── src/                        # Código fonte principal
│   ├── __init__.py
│   ├── dashboard_sinan_real_data.py    # Dashboard Streamlit principal
│   ├── processors/             # Processadores de dados
│   │   ├── __init__.py
│   │   ├── sinan_data_processor_comprehensive.py
│   │   └── sinan_data_processor_duckdb.py
│   └── utils/                  # Utilitários
│       ├── __init__.py
│       └── munic_dict_loader.py
│
├── notebooks/                  # Jupyter Notebooks
│   └── analise_performance_dados.ipynb
│
├── scripts/                    # Scripts de análise temporários
│   ├── explore_columns.py
│   └── analise_status_casos.py
│
├── data/                       # Dados do projeto
│   ├── raw/                    # Dados brutos
│   │   └── VIOLBR-PARQUET/    # Arquivos Parquet com dados SINAN
│   └── config/                 # Arquivos de configuração
│       └── TAB_SINANONLINE/    # Arquivos .cnv com dicionários SINAN
│
├── docs/                       # Documentação
│   ├── README_DASHBOARD.md
│   ├── README_DUCKDB.md
│   └── ANALISE_TECNICA_COMPLETA.md
│
└── reports/                    # Relatórios gerados
    ├── relatorio_colunas_sinan.txt
    └── relatorio_status_casos.txt
```

## 🚀 Como Usar

### 1. Instalação

```bash
# Instalar dependências
pip install -r requirements.txt
```

### 2. Pré-processar Dados (Opcional mas Recomendado)

Para acelerar o carregamento do dashboard, você pode pré-processar os dados uma vez:

```bash
# Pré-processar dados (gera arquivo otimizado)
python scripts/preprocess_data.py
```

**Benefícios:**

- ⚡ Carregamento muito mais rápido do dashboard
- 💾 Dados já processados e otimizados
- 🎯 Ideal para compartilhar o projeto (dados já prontos)

**Nota:** Se você não pré-processar, o dashboard processará os dados na primeira execução (pode demorar alguns minutos).

### 3. Executar o Dashboard

**Opção 1: Usando o script (Recomendado)**

# Ou Python (funciona em qualquer OS):
python run_dashboard.py
```

**Opção 2: Comando direto**

```bash
# A partir da raiz do projeto
streamlit run src/dashboard_sinan_real_data.py
```

### 3. Executar Análises

```bash
# Scripts de análise
python scripts/explore_columns.py
python scripts/analise_status_casos.py

# Notebooks Jupyter
python -m notebook notebooks/analise_performance_dados.ipynb
# Ou use JupyterLab (mais moderno):
python -m jupyterlab
```

## 📊 Funcionalidades

- **Dashboard Interativo**: Visualização de dados SINAN com filtros dinâmicos
- **Processamento Otimizado**: Suporte a DuckDB para queries rápidas em grandes volumes
- **Análises Estatísticas**: Distribuições, tendências e padrões
- **Análise de Performance**: Identificação de gargalos e otimizações

## 🔧 Tecnologias

- **Streamlit**: Interface web interativa
- **Pandas**: Manipulação de dados
- **DuckDB**: Queries otimizadas (opcional)
- **Plotly**: Visualizações interativas
- **Jupyter**: Análises exploratórias

## 📝 Documentação Adicional

- [Documentação do Dashboard](docs/README_DASHBOARD.md)
- [Documentação DuckDB](docs/README_DUCKDB.md)
- [Análise Técnica Completa](docs/ANALISE_TECNICA_COMPLETA.md)

## 📌 Notas

- Os dados devem estar na pasta `data/raw/VIOLBR-PARQUET/`
- Os arquivos de configuração (.cnv) devem estar em `data/config/TAB_SINANONLINE/`
- O dashboard suporta cache para melhor performance

## 🤝 Contribuindo

1. Mantenha a estrutura de pastas organizada
2. Adicione documentação para novas funcionalidades
3. Use os scripts em `scripts/` para análises temporárias
4. Mantenha notebooks em `notebooks/` para análises exploratórias
