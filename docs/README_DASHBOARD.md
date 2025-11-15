# Dashboard SINAN - Dados Reais

## Descrição

Dashboard interativo para análise de notificações de violência contra crianças e adolescentes usando dados reais do SINAN (Sistema de Informação de Agravos de Notificação).

## Características

- ✅ **Dados Reais**: Utiliza arquivos parquet do SINAN (2019-2024)
- ✅ **Sem Cruzamento com IBGE**: Análise pura dos dados do SINAN
- ✅ **Conformidade LGPD**: Dados agregados e anonimizados
- ✅ **Visualizações Interativas**: Gráficos Plotly responsivos
- ✅ **Filtros Avançados**: Ano, UF, Município, Tipo de Violência

## Hipóteses Testadas

O dashboard foi desenvolvido para testar as seguintes hipóteses de pesquisa:

- **H1, H10**: Tendência temporal de notificações
- **H2, H4, H5**: Distribuição demográfica (faixa etária e sexo)
- **H6, H7**: Distribuição geográfica (municípios e UFs)
- **H9**: Composição por tipo de violência

## Requisitos

```bash
pip install streamlit pandas plotly
```

## Como Executar

```bash
streamlit run dashboard_sinan_real_data.py
```

## Estrutura de Dados

O dashboard espera os seguintes arquivos:

```
VIOLBR-PARQUET/
  ├── VIOLBR19.parquet
  ├── VIOLBR20.parquet
  ├── VIOLBR21.parquet
  ├── VIOLBR22.parquet
  ├── VIOLBR23.parquet
  └── VIOLBR24.parquet

TAB_SINANONLINE/
  └── (arquivos .cnv para decodificação)
```

## Funcionalidades

### 1. Indicadores Principais (KPIs)
- Total de notificações
- Média anual
- Tipo de violência mais frequente
- Sexo mais frequente

### 2. Visualizações

#### Tendência Temporal (H1, H10)
- Gráfico de linhas mostrando evolução ao longo dos anos

#### Composição por Tipo de Violência (H9)
- Gráfico de barras empilhadas por ano e tipo

#### Distribuição Demográfica (H2, H4, H5)
- Gráfico de barras agrupadas por faixa etária e sexo

#### Distribuição Geográfica (H6, H7)
- Gráfico de barras por UF ou município

#### Local de Ocorrência
- Gráfico de pizza com os principais locais

#### Perfil do Agressor
- Distribuição por sexo do agressor

### 3. Filtros Interativos

- **Período**: Slider para selecionar intervalo de anos
- **UF**: Dropdown para filtrar por Unidade Federativa
- **Município**: Dropdown (apenas quando UF selecionada)
- **Tipo de Violência**: Dropdown para filtrar por tipo

## Notas Importantes

1. **Performance**: O dashboard usa cache do Streamlit para otimizar o carregamento
2. **LGPD**: Todos os dados são agregados e anonimizados
3. **Dados**: Apenas dados de violência contra crianças e adolescentes (0-17 anos) são exibidos
4. **Sem IBGE**: Este dashboard não cruza dados com informações do IBGE

## Estrutura do Código

- `load_sinan_data()`: Carrega e processa dados dos arquivos parquet
- `SINANDataProcessorComprehensive`: Classe para processar dados do SINAN
- Visualizações: Criadas com Plotly Express
- Filtros: Implementados na sidebar do Streamlit

## Troubleshooting

### Erro ao carregar dados
- Verifique se os arquivos parquet estão na pasta `VIOLBR-PARQUET/`
- Verifique se os arquivos não estão corrompidos

### Gráficos vazios
- Verifique se os filtros selecionados não estão muito restritivos
- Verifique se há dados para o período/região selecionada

### Performance lenta
- O primeiro carregamento pode ser lento devido ao processamento dos dados
- O cache do Streamlit acelera carregamentos subsequentes



