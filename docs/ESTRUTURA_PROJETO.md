# Estrutura do Projeto SINAN - Big Data Python

## Visão Geral

Este projeto implementa um dashboard interativo para análise de dados de violência contra crianças e adolescentes do Sistema de Informação de Agravos de Notificação (SINAN), utilizando Python e tecnologias modernas de Big Data.

## Estrutura de Diretórios

```
SINAN-BIG-DATA-PYTHON/
│
├── data/                          # Dados do projeto
│   ├── config/                    # Arquivos de configuração e dicionários
│   │   └── TAB_SINANONLINE/       # Dicionários de decodificação (.cnv)
│   │       ├── Munic*.cnv         # Dicionários de municípios por estado
│   │       ├── Sexo.cnv           # Dicionário de sexo
│   │       ├── Raça.cnv           # Dicionário de raça/cor
│   │       └── ...                 # Outros dicionários de decodificação
│   │
│   ├── processed/                 # Dados processados e pré-processados
│   │   ├── sinan_data_processed.parquet  # Dados pré-processados (cache)
│   │   └── metadata.json          # Metadados dos dados processados
│   │
│   └── raw/                       # Dados brutos
│       └── VIOLBR-PARQUET/        # Arquivos Parquet de violência
│           ├── VIOLBR19.parquet   # Dados de 2019
│           ├── VIOLBR20.parquet   # Dados de 2020
│           ├── VIOLBR21.parquet   # Dados de 2021
│           ├── VIOLBR22.parquet   # Dados de 2022
│           ├── VIOLBR23.parquet   # Dados de 2023
│           └── VIOLBR24.parquet   # Dados de 2024
│
├── src/                           # Código-fonte principal
│   ├── __init__.py
│   ├── dashboard_sinan_real_data.py  # Dashboard principal Streamlit
│   │
│   ├── processors/                # Processadores de dados
│   │   ├── __init__.py
│   │   ├── sinan_data_processor_comprehensive.py  # Processador completo (Pandas)
│   │   └── sinan_data_processor_duckdb.py         # Processador otimizado (DuckDB)
│   │
│   └── utils/                     # Utilitários
│       ├── __init__.py
│       └── munic_dict_loader.py   # Carregador de dicionários de municípios
│
├── scripts/                       # Scripts auxiliares
│   ├── preprocess_data.py         # Script de pré-processamento
│   ├── explore_columns.py         # Exploração de colunas
│   └── analise_status_casos.py    # Análise de status dos casos
│
├── notebooks/                     # Jupyter Notebooks
│   └── analise_performance_dados.ipynb  # Análise de performance
│
├── docs/                          # Documentação
│   ├── ESTRUTURA_PROJETO.md       # Este arquivo
│   ├── ANALISE_TECNICA.md         # Análise técnica detalhada
│   ├── README_DASHBOARD.md        # Documentação do dashboard
│   ├── README_DUCKDB.md           # Documentação do DuckDB
│   └── ...
│
├── reports/                       # Relatórios gerados
│   ├── relatorio_colunas_sinan.txt
│   └── relatorio_status_casos.txt
│
├── run_dashboard.py               # Script principal de execução
├── start_railway.py               # Script para deploy Railway
├── limpar_cache_streamlit.py      # Script para limpar cache
├── requirements.txt               # Dependências do projeto
├── pyrightconfig.json             # Configuração do Pyright (type checking)
├── railway.json                   # Configuração Railway
└── README.md                      # README principal
```

## Descrição Detalhada dos Componentes

### 1. Diretório `data/`

#### `data/config/TAB_SINANONLINE/`

Contém arquivos de configuração no formato `.cnv` (Conversão) do SINAN:

- **Munic\*.cnv**: Dicionários de municípios por estado (ex: `Municma.cnv` para Maranhão)
- **Sexo.cnv**: Mapeamento de códigos de sexo
- **Raça.cnv**: Mapeamento de raça/cor
- **UFcodigo.cnv**: Códigos de unidades federativas
- Outros dicionários de decodificação de campos do SINAN

**Formato dos arquivos .cnv:**

```
; Comentário
1  110001 Alta Floresta D'Oeste  110001
2  110002 Ariquemes  110002
...
```

#### `data/raw/VIOLBR-PARQUET/`

Arquivos Parquet contendo dados brutos de violência:

- Formato: Apache Parquet (otimizado para Big Data)
- Estrutura: Um arquivo por ano (2019-2024)
- Tamanho: Arquivos grandes (milhões de registros)
- Schema: Colunas do SINAN (ex: `NU_IDADE_N`, `DT_NOTIFIC`, `VIOL_SEXU`, etc.)

#### `data/processed/`

Dados pré-processados para acelerar o carregamento:

- **sinan_data_processed.parquet**: Dados já filtrados e transformados
- **metadata.json**: Informações sobre o processamento (data, versão, etc.)

### 2. Diretório `src/`

#### `src/dashboard_sinan_real_data.py`

**Arquivo principal do dashboard Streamlit** (~2700 linhas)

**Estrutura principal:**

- Imports e configuração inicial
- CSS personalizado e JavaScript para responsividade
- Funções auxiliares de formatação
- Função de carregamento de dados (`load_sinan_data`)
- Função de criação de colunas derivadas (`create_derived_columns`)
- Interface do dashboard (filtros, KPIs, gráficos)
- 10+ gráficos interativos usando Plotly

**Principais seções:**

1. Configuração e estilos
2. Carregamento de dados (com cache)
3. Filtros interativos (ano, UF, município, tipo de violência)
4. KPIs principais
5. Gráficos de análise (H1-H10)

#### `src/processors/`

##### `sinan_data_processor_comprehensive.py`

Processador completo usando Pandas:

- **Classe**: `SINANDataProcessorComprehensive`
- **Responsabilidades**:
  - Carregar dicionários de decodificação
  - Carregar dados de violência de arquivos Parquet
  - Aplicar dicionários para decodificar códigos
  - Filtrar dados (idade, tipo de violência)
  - Processar relacionamentos e parentescos

**Métodos principais:**

- `load_dictionaries()`: Carrega dicionários básicos
- `load_violence_data()`: Carrega todos os arquivos Parquet
- `apply_dictionaries()`: Decodifica códigos usando dicionários
- `filter_comprehensive_violence()`: Filtra violência contra crianças/adolescentes

##### `sinan_data_processor_duckdb.py`

Processador otimizado usando DuckDB:

- **Classe**: `SINANDataProcessorDuckDB`
- **Context Manager**: Usa `__enter__` e `__exit__` para gerenciar conexão
- **Responsabilidades**:
  - Consultas SQL diretas em arquivos Parquet
  - Filtros eficientes antes de carregar na memória
  - Performance superior para grandes volumes

**Métodos principais:**

- `query_with_filters()`: Executa SQL com filtros
- `load_filtered_violence_data()`: Carrega dados já filtrados
- `get_parquet_files()`: Lista arquivos disponíveis

#### `src/utils/`

##### `munic_dict_loader.py`

Utilitário para carregar dicionários de municípios:

- **Função**: `load_municipality_dict()`
- **Funcionalidade**: Lê arquivos `.cnv` e extrai códigos/nomes de municípios
- **Retorno**: Dicionário `{codigo: nome_municipio}`

### 3. Diretório `scripts/`

Scripts auxiliares para processamento e análise:

- **preprocess_data.py**: Pré-processa dados e salva em Parquet
- **explore_columns.py**: Explora e lista colunas disponíveis
- **analise_status_casos.py**: Análise específica de status de casos

### 4. Arquivos de Configuração

#### `requirements.txt`

Lista de dependências:

- **Essenciais**: pandas, numpy, streamlit, plotly, pyarrow
- **Performance**: duckdb (opcional)
- **Desenvolvimento**: psutil, jupyter, jupyterlab

#### `run_dashboard.py`

Script principal de execução:

- Verifica existência do dashboard
- Executa Streamlit via subprocess
- Tratamento de erros e interrupções

#### `pyrightconfig.json`

Configuração do Pyright para type checking estático

#### `railway.json`

Configuração para deploy no Railway (PaaS)

## Fluxo de Dados

```
1. Dados Brutos (Parquet)
   ↓
2. Processador (Pandas ou DuckDB)
   ├── Carrega dicionários (.cnv)
   ├── Lê arquivos Parquet
   ├── Aplica filtros (idade, violência)
   └── Decodifica códigos
   ↓
3. Dados Processados
   ├── Cache (processed/sinan_data_processed.parquet)
   └── DataFrame em memória
   ↓
4. Dashboard Streamlit
   ├── Carrega dados (com cache)
   ├── Cria colunas derivadas
   ├── Aplica filtros do usuário
   └── Gera visualizações
```

## Padrões de Organização

### Nomenclatura

- **Arquivos Python**: snake_case (ex: `sinan_data_processor.py`)
- **Classes**: PascalCase (ex: `SINANDataProcessorComprehensive`)
- **Funções**: snake_case (ex: `load_violence_data()`)
- **Constantes**: UPPER_CASE (ex: `DUCKDB_AVAILABLE`)

### Estrutura de Código

- **Imports**: Bibliotecas padrão → Bibliotecas de terceiros → Imports locais
- **Docstrings**: Formato Google/NumPy
- **Comentários**: Em português para contexto de negócio

### Separação de Responsabilidades

- **Processadores**: Lógica de processamento de dados
- **Utils**: Funções auxiliares reutilizáveis
- **Dashboard**: Interface e visualização
- **Scripts**: Automação e tarefas pontuais

## Dependências entre Módulos

```
dashboard_sinan_real_data.py
    ├── processors/
    │   ├── sinan_data_processor_comprehensive.py
    │   └── sinan_data_processor_duckdb.py
    │       └── sinan_data_processor_comprehensive.py (herança)
    └── utils/
        └── munic_dict_loader.py
```

## Estrutura de Dados

### Schema dos Dados SINAN (Principais Colunas)

**Identificação:**

- `DT_NOTIFIC`: Data de notificação
- `NU_ANO`: Ano da notificação
- `SG_UF_NOT`: UF de notificação
- `ID_MUNICIP`: Código do município

**Vítima:**

- `NU_IDADE_N`: Idade (código: 4000-4017 para 0-17 anos)
- `CS_SEXO`: Sexo (1=Masculino, 2=Feminino)
- `CS_RACA`: Raça/Cor

**Violência:**

- `VIOL_FISIC`: Violência física (1=Sim, 2=Não)
- `VIOL_PSICO`: Violência psicológica
- `VIOL_SEXU`: Violência sexual
- `VIOL_INFAN`: Violência infantil

**Agressor:**

- `AUTOR_SEXO`: Sexo do agressor
- `REL_*`: Colunas de relacionamento (REL_PAI, REL_MAE, etc.)

**Outros:**

- `LOCAL_OCOR`: Local de ocorrência
- `DT_OCOR`: Data de ocorrência
- `ENC_*`: Encaminhamentos

### Colunas Derivadas Criadas

O dashboard cria colunas derivadas para facilitar análises:

- `ANO_NOTIFIC`: Ano extraído da data
- `UF_NOTIFIC`: Nome da UF (decodificado)
- `MUNICIPIO_NOTIFIC`: Nome do município (decodificado)
- `FAIXA_ETARIA`: Faixa etária (0-1, 2-5, 6-9, 10-13, 14-17 anos)
- `SEXO`: Sexo decodificado
- `TIPO_VIOLENCIA`: Tipo(s) de violência (pode ser combinado)
- `GRAU_PARENTESCO`: Relacionamento com agressor
- `AUTOR_SEXO_CORRIGIDO`: Sexo do agressor corrigido

## Cache e Performance

### Estratégias de Cache

1. **Streamlit Cache**: `@st.cache_data` para dados carregados
2. **Dados Pré-processados**: Arquivo Parquet em `data/processed/`
3. **Filtros Antecipados**: DuckDB filtra antes de carregar na memória

### Otimizações

- Carregamento lazy (apenas quando necessário)
- Filtros aplicados antes de processar tudo
- Uso de tipos de dados eficientes (categorical, datetime)
- Processamento em lotes quando possível

## Extensibilidade

### Adicionar Novo Gráfico

1. Criar seção no dashboard
2. Adicionar filtros se necessário
3. Processar dados específicos
4. Criar visualização Plotly
5. Adicionar análise/KPIs relacionados

### Adicionar Novo Processador

1. Criar classe em `src/processors/`
2. Implementar interface similar aos existentes
3. Importar no dashboard
4. Adicionar opção de seleção

### Adicionar Novo Tipo de Análise

1. Criar função de processamento
2. Adicionar seção no dashboard
3. Criar visualizações apropriadas
4. Documentar hipóteses (H1, H2, etc.)

## Manutenção

### Limpeza de Cache

- Script: `limpar_cache_streamlit.py`
- Localização: `.streamlit/cache/` (criado automaticamente)

### Atualização de Dados

1. Adicionar novos arquivos Parquet em `data/raw/VIOLBR-PARQUET/`
2. Limpar cache processado: `data/processed/sinan_data_processed.parquet`
3. Reiniciar dashboard para reprocessar

### Logs e Debug

- Mensagens de log via `print()` com prefixos `[INFO]`, `[OK]`, `[ERRO]`
- Tratamento de exceções com traceback completo
- Mensagens de erro amigáveis no Streamlit
