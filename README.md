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
│   ├── ANALISE_TECNICA.md
│   ├── ESTRUTURA_PROJETO.md
│   ├── COMO_USAR_NOTEBOOKS.md
│   └── COMANDOS_JUPYTER.md
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

```bash
# Execute o script Python (funciona em qualquer OS):
python run_dashboard.py
```

**Opção 2: Comando direto**

```bash
# A partir da raiz do projeto
streamlit run src/dashboard_sinan_real_data.py
```

O dashboard será aberto automaticamente no seu navegador padrão, geralmente em `http://localhost:8501`.

### 4. Executar Análises

```bash
# Scripts de análise
python scripts/explore_columns.py
python scripts/analise_status_casos.py

# Notebooks Jupyter
# Instale o Jupyter primeiro: pip install jupyter jupyterlab
jupyter notebook notebooks/analise_performance_dados.ipynb
# Ou use JupyterLab (mais moderno):
jupyter lab
```

**Nota:** Para mais informações sobre como usar os notebooks, consulte [COMO_USAR_NOTEBOOKS.md](docs/COMO_USAR_NOTEBOOKS.md).

## 📊 Funcionalidades

- **Dashboard Interativo**: Visualização de dados SINAN com filtros dinâmicos
- **Processamento Otimizado**: Suporte a DuckDB para queries rápidas em grandes volumes (opcional)
- **Análises Estatísticas**: Distribuições, tendências e padrões
- **Validação de Hipóteses**: Teste de 10 hipóteses específicas sobre violência contra crianças e adolescentes
- **Análises Temporais**: Identificação de tendências e padrões ao longo do tempo
- **Análises Demográficas**: Distribuição por faixa etária, sexo e raça/cor
- **Análises Geográficas**: Distribuição por municípios e estados

## 🔧 Tecnologias

- **Streamlit**: Interface web interativa
- **Pandas**: Manipulação de dados
- **DuckDB**: Queries otimizadas (opcional)
- **Plotly**: Visualizações interativas
- **Jupyter**: Análises exploratórias

## 📝 Documentação Adicional

- [Documentação do Dashboard](docs/README_DASHBOARD.md) - Guia completo sobre o dashboard e suas funcionalidades
- [Documentação DuckDB](docs/README_DUCKDB.md) - Como usar DuckDB para melhor performance
- [Análise Técnica](docs/ANALISE_TECNICA.md) - Análise técnica detalhada do projeto
- [Estrutura do Projeto](docs/ESTRUTURA_PROJETO.md) - Detalhes sobre a organização do código
- [Como Usar Notebooks](docs/COMO_USAR_NOTEBOOKS.md) - Guia para trabalhar com Jupyter Notebooks

## 📌 Notas

- Os dados devem estar na pasta `data/raw/VIOLBR-PARQUET/`
- Os arquivos de configuração (.cnv) devem estar em `data/config/TAB_SINANONLINE/`
- O dashboard suporta cache para melhor performance

## 🤝 Contribuindo

1. Mantenha a estrutura de pastas organizada
2. Adicione documentação para novas funcionalidades
3. Use os scripts em `scripts/` para análises temporárias
4. Mantenha notebooks em `notebooks/` para análises exploratórias
