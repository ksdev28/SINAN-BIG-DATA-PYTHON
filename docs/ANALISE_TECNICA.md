# Análise Técnica do Projeto SINAN - Big Data Python

## Visão Geral Técnica

Este documento apresenta uma análise técnica detalhada do projeto, incluindo frameworks, bibliotecas, arquitetura de dados, processamento e otimizações.

## Stack Tecnológico

### Frameworks e Bibliotecas Principais

#### 1. Streamlit (Framework Web)

**Versão**: >= 1.49.0  
**Papel**: Framework principal para criação do dashboard interativo

**Uso no Projeto:**

- Interface web reativa e interativa
- Componentes: `st.sidebar`, `st.columns`, `st.metric`, `st.plotly_chart`
- Cache de dados: `@st.cache_data` para otimização
- Gerenciamento de estado: Sessão do Streamlit

**Funções Principais Utilizadas:**

```python
st.set_page_config()      # Configuração da página
st.cache_data()           # Cache de dados (TTL configurável)
st.sidebar.*              # Componentes da sidebar
st.columns()              # Layout em colunas
st.metric()               # KPIs e métricas
st.plotly_chart()         # Gráficos Plotly
st.dataframe()            # Tabelas de dados
st.download_button()      # Download de dados
```

**Vantagens:**

- Desenvolvimento rápido (Python puro)
- Sem necessidade de frontend separado
- Cache automático integrado
- Deploy simples

#### 2. Pandas (Manipulação de Dados)

**Versão**: >= 2.2.0  
**Papel**: Biblioteca principal para manipulação e análise de dados

**Uso no Projeto:**

- Leitura de arquivos Parquet: `pd.read_parquet()`
- Manipulação de DataFrames
- Agregações: `groupby()`, `value_counts()`, `agg()`
- Transformações: `apply()`, `map()`, `transform()`
- Filtros: `query()`, boolean indexing
- Operações de data: `pd.to_datetime()`, `dt.year`, `dt.to_period()`

**Operações Críticas:**

```python
# Carregamento
df = pd.read_parquet(file_path)

# Agregações
df.groupby(['ANO_NOTIFIC', 'TIPO_VIOLENCIA']).size()

# Transformações
df['ANO_NOTIFIC'] = df['DT_NOTIFIC'].dt.year

# Filtros
df_filtrado = df[df['NU_IDADE_N'].isin(age_codes)]
```

**Otimizações:**

- Uso de tipos categóricos para strings repetidas
- Operações vetorizadas (evitar loops)
- Filtros aplicados antes de processar

#### 3. Plotly (Visualização)

**Versão**: >= 5.15.0  
**Papel**: Biblioteca de visualização interativa

**Componentes Utilizados:**

- **Plotly Express (`px`)**: API de alto nível
  - `px.line()`: Gráficos de linha
  - `px.bar()`: Gráficos de barras
  - `px.bar()` com `barmode='stack'`: Barras empilhadas
- **Plotly Graph Objects (`go`)**: API de baixo nível (usado para anotações)

**Gráficos Implementados:**

1. **Tendência Temporal (H1, H10)**: `px.line()` com anotações
2. **Composição por Tipo (H9)**: `px.bar()` empilhado
3. **Distribuição Demográfica (H2, H4, H5)**: `px.bar()` agrupado
4. **Distribuição Geográfica (H6, H7)**: `px.bar()` horizontal/vertical
5. **Local de Ocorrência**: `px.bar()` horizontal com colorbar
6. **Perfil do Agressor (H3)**: `px.bar()` simples
7. **Relacionamento (H8)**: `px.bar()` horizontal com colorbar
8. **Raça/Cor (H4)**: `px.bar()` com colorbar
9. **Evolução Mensal (H10)**: `px.line()` simplificado

**Customizações:**

- Layouts responsivos com JavaScript
- Formatação brasileira de números
- Anotações dinâmicas
- Hover templates personalizados
- Colorbar adaptativa (desktop/mobile)

#### 4. DuckDB (Processamento Otimizado)

**Versão**: >= 0.9.0  
**Papel**: Motor SQL analítico para consultas eficientes em Parquet

**Vantagens:**

- Consultas SQL diretas em arquivos Parquet (sem carregar tudo)
- Filtros aplicados antes de carregar na memória
- Performance superior para grandes volumes
- Compatível com Pandas (retorna DataFrames)

**Uso no Projeto:**

```python
# Conexão
conn = duckdb.connect()

# Consulta SQL em Parquet
query = """
    SELECT * FROM read_parquet('data/raw/VIOLBR-PARQUET/VIOLBR19.parquet')
    WHERE NU_IDADE_N IN ('4000', '4001', ..., '4017')
    AND (VIOL_FISIC = '1' OR VIOL_PSICO = '1' OR VIOL_SEXU = '1')
"""
df = conn.execute(query).df()
```

**Estratégia de Uso:**

- Fallback automático: Se DuckDB não disponível, usa Pandas
- Filtros aplicados na query SQL
- Apenas colunas necessárias são selecionadas

#### 5. PyArrow (Formato Parquet)

**Versão**: >= 10.0.0  
**Papel**: Biblioteca para leitura/escrita de arquivos Parquet

**Uso:**

- Leitura eficiente de Parquet (usado por Pandas e DuckDB)
- Compressão automática
- Schema preservation
- Tipos de dados otimizados

### Bibliotecas Auxiliares

#### NumPy

**Versão**: >= 2.3.0  
**Uso**: Operações numéricas, arrays (usado internamente por Pandas)

#### Pathlib

**Uso**: Manipulação de caminhos de arquivos (Python 3.4+)

```python
from pathlib import Path
project_root = Path(__file__).parent.parent
data_path = project_root / "data" / "raw"
```

#### Collections

**Uso**: `Counter` para contagem de relacionamentos

```python
from collections import Counter
parentesco_counts = Counter(parentescos_list)
```

#### Re (Regex)

**Uso**: Processamento de strings, extração de padrões

```python
import re
match = re.search(r'(\d{1,2})\s*ano', age_str, re.IGNORECASE)
```

## Arquitetura de Processamento de Dados

### Fluxo de Carregamento

#### 1. Carregamento Inicial (`load_sinan_data()`)

**Estratégia Multi-Camada:**

```python
@st.cache_data(ttl=3600, max_entries=1)
def load_sinan_data(use_duckdb=True, use_preprocessed=True):
    # Camada 1: Tentar dados pré-processados
    if use_preprocessed:
        processed_file = "data/processed/sinan_data_processed.parquet"
        if processed_file.exists():
            return pd.read_parquet(processed_file)

    # Camada 2: DuckDB (se disponível)
    if use_duckdb and DUCKDB_AVAILABLE:
        with SINANDataProcessorDuckDB(...) as processor:
            return processor.load_filtered_violence_data(...)

    # Camada 3: Pandas (fallback)
    processor = SINANDataProcessorComprehensive(...)
    return processor.load_violence_data()
```

**Otimizações:**

- Cache do Streamlit (TTL: 1 hora)
- Dados pré-processados (mais rápido)
- DuckDB para grandes volumes
- Fallback para Pandas

#### 2. Processamento de Dicionários

**Carregamento de Dicionários (.cnv):**

```python
def load_dictionaries():
    # Dicionários hardcoded (campos principais)
    dictionaries = {
        'CS_SEXO': {'1': 'Masculino', '2': 'Feminino', ...},
        'VIOL_FISIC': {'1': 'Sim', '2': 'Não', ...},
        ...
    }

    # Dicionários de arquivos .cnv (municípios)
    municip_dict = load_municipality_dict()
```

**Processamento de Municípios:**

- Lê arquivos `.cnv` com encoding `latin-1`
- Extrai padrão: `código nome_município código`
- Regex para parsing
- Retorna dicionário `{codigo: nome}`

#### 3. Aplicação de Dicionários

**Decodificação de Códigos:**

```python
def apply_dictionaries(df):
    for column, mapping in dictionaries.items():
        if column in df.columns:
            df[column] = df[column].astype(str).map(mapping)
    return df
```

**Estratégia:**

- Mapeamento direto usando `.map()`
- Tratamento de valores ausentes
- Preservação de valores não mapeados

#### 4. Filtros Aplicados

**Filtro por Idade (0-17 anos):**

```python
# Códigos de idade: 4000 (menor de 1 ano) até 4017 (17 anos)
age_codes = [f'400{i}' if i < 10 else f'40{i}' for i in range(0, 18)]
age_filter = df['NU_IDADE_N'].isin(age_codes)
child_data = df[age_filter]
```

**Filtro por Violência:**

```python
def tem_violencia(row):
    for col in ['VIOL_FISIC', 'VIOL_PSICO', 'VIOL_SEXU', 'VIOL_INFAN']:
        if str(row[col]).upper() in ['1', 'SIM', 'S']:
            return True
    return False

df_violencia = df[df.apply(tem_violencia, axis=1)]
```

**Otimização:**

- Filtros aplicados ANTES de decodificar (menos dados)
- Operações vetorizadas quando possível
- Uso de boolean indexing

### Criação de Colunas Derivadas

**Função: `create_derived_columns(df)`**

**Colunas Criadas:**

1. **Datas:**

```python
df['DT_NOTIFIC'] = pd.to_datetime(df['DT_NOTIFIC'], format='%Y%m%d')
df['ANO_NOTIFIC'] = df['DT_NOTIFIC'].dt.year
```

2. **UF e Município:**

```python
df['UF_NOTIFIC'] = df['SG_UF_NOT'].apply(map_uf)
df['MUNICIPIO_NOTIFIC'] = df['ID_MUNICIP'].apply(map_municipio)
```

3. **Tipo de Violência:**

```python
def get_violence_type(row):
    types = []
    if row['VIOL_FISIC'] == '1': types.append('Física')
    if row['VIOL_PSICO'] == '1': types.append('Psicológica')
    if row['VIOL_SEXU'] == '1': types.append('Sexual')
    return ', '.join(types) if types else 'Não especificado'
```

4. **Faixa Etária:**

```python
def get_age_group(age_str):
    # Extrai número da idade do código
    # Classifica em: 0-1, 2-5, 6-9, 10-13, 14-17 anos
```

5. **Relacionamento:**

```python
def get_relacionamento(row):
    relacionamentos = []
    for col in rel_cols:  # REL_PAI, REL_MAE, etc.
        if row[col] == '1':
            relacionamentos.append(RELACIONAMENTO_DICT[col])
    return ', '.join(relacionamentos)
```

## Estratégias de Performance

### 1. Cache Multi-Camada

**Nível 1: Streamlit Cache**

```python
@st.cache_data(ttl=3600, max_entries=1)
def load_sinan_data(...):
    # Cacheado por 1 hora
```

**Nível 2: Dados Pré-processados**

- Arquivo Parquet em `data/processed/`
- Evita reprocessamento completo
- Verificação de integridade

**Nível 3: Cache de Dicionários**

```python
@st.cache_data(ttl=86400)  # 24 horas
def load_municipality_dictionary():
    # Dicionários não mudam frequentemente
```

### 2. Filtros Antecipados

**DuckDB:**

- Filtros aplicados na query SQL
- Apenas dados necessários são carregados
- Reduz uso de memória

**Pandas:**

- Filtrar por idade ANTES de decodificar
- Selecionar apenas colunas necessárias
- Processar em lotes quando possível

### 3. Otimização de Memória

**Técnicas:**

- Uso de `copy(deep=False)` quando possível
- Liberação explícita: `del df_large`
- Tipos de dados eficientes (categorical, int8, etc.)
- Processamento incremental

**Exemplo:**

```python
# Shallow copy (mais eficiente)
df = child_data.copy(deep=False)

# Liberar memória
del violence_data
del child_data_raw
```

### 4. Processamento Lazy

- Dados carregados apenas quando necessário
- Gráficos gerados sob demanda
- Filtros aplicados dinamicamente

## Funções Auxiliares

### Formatação

**`formatar_numero_br(numero)`**

- Formata números no padrão brasileiro
- Ponto para milhares, vírgula para decimais
- Tratamento de NaN

**`formatar_numero_compacto(numero)`**

- Formata números grandes com sufixos (k, M, B)
- Exemplo: 78107 → 78.107k

**`gerar_tick_values(min_value, max_value, max_ticks=8)`**

- Gera valores para ticks de eixo
- Distribuição uniforme
- Arredondamento inteligente

**`aplicar_formatacao_eixo(figura, max_value, eixo='y')`**

- Aplica formatação brasileira aos eixos
- Usa `formatar_numero_br()` ou `formatar_numero_compacto()`

### Processamento de Dados

**`process_date_column(df, col_name)`**

- Processa colunas de data no formato YYYYMMDD
- Conversão para datetime
- Tratamento de erros

**`get_violence_type(row)`**

- Identifica tipos de violência em um registro
- Suporta múltiplos tipos (combinados)
- Retorna string formatada

**`get_age_group(age_str)`**

- Extrai idade de código SINAN
- Classifica em faixas etárias
- Tratamento de casos especiais

## Responsividade e UX

### CSS Personalizado

**Estilos Implementados:**

- Layout monocromático (preto/branco)
- Espaçamento otimizado
- Responsividade mobile
- Ajuste de colorbars dinamicamente

**JavaScript para Responsividade:**

```javascript
function adjustColorbarsForMobile() {
  const isMobile = window.innerWidth <= 767;
  // Ajusta colorbars para horizontal em mobile
}
```

### Layout Adaptativo

**Desktop:**

- 4 colunas para KPIs
- Colorbar vertical
- Layout amplo

**Mobile:**

- 2x2 para KPIs (CSS)
- Colorbar horizontal
- Margens ajustadas

## Tratamento de Erros

### Estratégias

1. **Try/Except com Fallback:**

```python
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    # Fallback para Pandas
```

2. **Validação de Dados:**

```python
if df is None or len(df) == 0:
    st.error("Não foi possível carregar os dados.")
    st.stop()
```

3. **Mensagens Informativas:**

- Logs com prefixos `[INFO]`, `[OK]`, `[ERRO]`
- Mensagens de erro amigáveis no Streamlit
- Traceback completo em modo debug

4. **Tratamento de MemoryError:**

```python
except MemoryError:
    st.cache_data.clear()
    st.warning("Cache limpo devido a erro de memória.")
    df, processor = load_sinan_data()
```

## Segurança e Conformidade

### LGPD (Lei Geral de Proteção de Dados)

**Implementações:**

- Dados agregados e anonimizados
- Nenhum dado pessoal identificável
- Nota de conformidade no dashboard
- Processamento apenas de dados públicos

### Validação de Entrada

- Filtros validados antes de aplicar
- Sanitização de inputs do usuário
- Tratamento de valores inválidos

## Deploy e Produção

### Configuração Railway

**Arquivo: `railway.json`**

- Configuração de build
- Variáveis de ambiente
- Comandos de start

### Scripts de Deploy

**`start_railway.py`**

- Adaptação para ambiente Railway
- Configuração de portas
- Tratamento de variáveis de ambiente

### Limpeza de Cache

**`limpar_cache_streamlit.py`**

- Remove cache do Streamlit
- Útil após atualizações
- Libera espaço em disco

## Métricas e Monitoramento

### Performance

**Indicadores:**

- Tempo de carregamento de dados
- Uso de memória
- Tamanho de cache
- Número de registros processados

**Logs:**

```python
print(f"[OK] Dados carregados: {len(df):,} registros")
print(f"[INFO] Tempo de processamento: {tempo:.2f}s")
```

## Extensibilidade Técnica

### Adicionar Nova Biblioteca

1. Adicionar em `requirements.txt`
2. Importar no código
3. Implementar funcionalidade
4. Testar compatibilidade

### Adicionar Novo Processador

1. Criar classe em `src/processors/`
2. Implementar interface padrão
3. Adicionar fallback no dashboard
4. Documentar uso

### Adicionar Nova Visualização

1. Criar função de processamento
2. Usar Plotly Express ou Graph Objects
3. Aplicar formatação brasileira
4. Adicionar responsividade
5. Integrar no dashboard

## Considerações de Escalabilidade

### Limitações Atuais

- Processamento em memória (limite de RAM)
- Cache local (não distribuído)
- Processamento síncrono

### Melhorias Futuras

- Processamento distribuído (Dask, Spark)
- Cache distribuído (Redis)
- Processamento assíncrono
- API separada do frontend
- Banco de dados para metadados

## Conclusão

O projeto utiliza uma stack moderna e eficiente para análise de Big Data:

- **Streamlit** para interface rápida
- **Pandas/DuckDB** para processamento eficiente
- **Plotly** para visualizações interativas
- **Parquet** para armazenamento otimizado
- **Cache multi-camada** para performance
- **Filtros antecipados** para otimização de memória

A arquitetura é modular, extensível e otimizada para grandes volumes de dados, mantendo boa performance e usabilidade.
