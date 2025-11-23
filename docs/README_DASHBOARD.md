# Dashboard SINAN: Análise de Notificações de Violência contra Crianças e Adolescentes

## 🎯 Propósito do Dashboard

Este dashboard foi desenvolvido para **analisar e visualizar dados reais de notificações de violência contra crianças e adolescentes** do SINAN (Sistema de Informação de Agravos de Notificação), permitindo a **validação de hipóteses de pesquisa** através de visualizações interativas e análises estatísticas.

### Objetivos Principais

1. **Análise de Tendências Temporais**: Identificar padrões e mudanças nas notificações ao longo do tempo, especialmente em relação ao período da pandemia (2020-2021)

2. **Análise Demográfica**: Compreender a distribuição das notificações por faixa etária, sexo e raça/cor das vítimas

3. **Análise Geográfica**: Avaliar a distribuição geográfica das notificações, com foco especial em municípios específicos (como Imperatriz no Maranhão)

4. **Análise por Tipo de Violência**: Identificar padrões e possíveis subnotificações, especialmente relacionadas à violência psicológica

5. **Análise do Perfil do Agressor**: Compreender características dos agressores e seu relacionamento com as vítimas

## 📊 Hipóteses de Pesquisa Testadas

O dashboard foi estruturado para testar **10 hipóteses específicas (H1-H10)** sobre violência contra crianças e adolescentes:

### H1 – Tendência Geral

**Hipótese:** "As notificações aumentaram após a pandemia?"

**Análise:** Gráfico de linha temporal mostrando a evolução das notificações ao longo dos anos (2019-2024), com destaque visual para o período da pandemia.

### H2 – Tipo de Violência por Faixa Etária

**Hipótese:** "Violência sexual é o tipo mais incidente entre adolescentes (12–17 anos)?"

**Análise:** Gráfico de barras agrupadas comparando tipos de violência (Física, Sexual, Psicológica) por faixa etária, com foco em adolescentes.

### H3 – Perfil do Agressor

**Hipótese:** "Qual é a distribuição por sexo dos agressores nas notificações de violência?"

**Análise:** Gráfico de barras mostrando a distribuição de agressores por sexo (masculino, feminino, ignorado).

### H4 – Perfil da Vítima

**Hipótese:** "Qual é a distribuição por raça/cor das vítimas de violência contra crianças e adolescentes?"

**Análise:** Gráfico de barras mostrando a distribuição das vítimas por raça/cor, permitindo identificar desproporcionalidades.

### H5 – Faixa Etária vs Psicológica/Moral

**Hipótese:** "A violência psicológica é mais comum em adolescentes de 15 a 17 anos?"

**Análise:** Comparação da incidência de violência psicológica entre diferentes faixas etárias, especialmente adolescentes.

### H6 – Comparação Regional

**Hipótese:** "Imperatriz tem taxa de notificação maior que municípios de tamanho semelhante?"

**Análise:** Gráfico comparativo mostrando Imperatriz em relação a municípios com número similar de notificações (±30% do valor de Imperatriz), permitindo avaliar se a taxa de notificação está acima ou abaixo da média.

### H7 – Contribuição Estadual

**Hipótese:** "Imperatriz representa mais de 15% das notificações do Maranhão?"

**Análise:** Gráfico de barras horizontais mostrando os principais municípios do Maranhão por número de notificações, destacando a posição e percentual de Imperatriz em relação ao total do estado.

### H8 – Relacionamento com o Agressor

**Hipótese:** "Qual é o grau de parentesco ou relacionamento mais comum entre a vítima e o agressor?"

**Análise:** Gráfico de barras mostrando a distribuição dos relacionamentos (pai, mãe, padrasto, desconhecido, etc.), identificando os padrões mais frequentes.

### H9 – Subnotificação

**Hipótese:** "A violência psicológica está sendo subnotificada?"

**Análise:** Comparação das proporções de violência física, sexual e psicológica, avaliando se a violência psicológica apresenta proporções significativamente menores, indicando possível subnotificação.

### H10 – Impacto da Pandemia

**Hipótese:** "Houve queda das notificações em 2020 e aumento em 2021?"

**Análise:**

- Gráfico de linha temporal com destaque visual para o período 2020-2021
- Gráfico mensal mostrando a evolução mês a mês durante o período da pandemia
- Análise de variações percentuais entre os meses

## 🔍 Funcionalidades do Dashboard

### 1. Indicadores Principais (KPIs)

- **Total de Notificações**: Número total de casos no período selecionado
- **Média Anual**: Média de notificações por ano
- **Tipo de Violência Mais Frequente**: Identificação do tipo mais comum
- **Sexo Mais Frequente**: Distribuição por sexo das vítimas

### 2. Visualizações Interativas

#### Tendência Temporal (H1, H10)

- Gráfico de linha mostrando evolução anual das notificações
- Destaque visual para período da pandemia (2020-2021)
- Gráfico mensal detalhado para análise do impacto da pandemia

#### Composição por Tipo de Violência (H9)

- Gráfico comparativo de barras mostrando proporções de Física, Sexual e Psicológica
- Análise textual sobre possível subnotificação

#### Distribuição Demográfica (H2, H4, H5)

- Gráfico de barras agrupadas por faixa etária e sexo
- Análise de tipos de violência por faixa etária
- KPIs específicos para cada hipótese

#### Distribuição Geográfica (H6, H7)

- **H6**: Comparação de Imperatriz com municípios de tamanho semelhante (todas as UFs)
- **H7**: Ranking dos municípios do Maranhão com destaque para Imperatriz
- Análise de posicionamento e percentuais

#### Perfil do Agressor (H3)

- Distribuição por sexo do agressor
- Análise de padrões de agressão

#### Relacionamento com o Agressor (H8)

- Distribuição por grau de parentesco/relacionamento
- Identificação dos relacionamentos mais frequentes

#### Perfil da Vítima (H4)

- Distribuição por raça/cor
- Análise de desproporcionalidades

#### Local de Ocorrência

- Gráfico de barras mostrando os principais locais onde ocorreram as violências

### 3. Filtros Interativos

O dashboard permite filtrar os dados por:

- **Período (Anos)**: Slider para selecionar intervalo de anos (2019-2024)
- **Unidade Federativa (UF)**: Dropdown para filtrar por estado
- **Município**: Dropdown disponível quando uma UF é selecionada
- **Tipo de Violência**: Filtro por tipo específico (Física, Sexual, Psicológica, etc.)

**Importante:** Os filtros são aplicados dinamicamente a todas as visualizações, permitindo análises segmentadas.

## 🚀 Como Usar

### Pré-requisitos

```bash
# Instalar dependências
pip install -r requirements.txt
```

### Executar o Dashboard

**Opção 1: Script de Execução (Recomendado)**

```bash
python run_dashboard.py
```

**Opção 2: Comando Direto**

```bash
streamlit run src/dashboard_sinan_real_data.py
```

### Estrutura de Dados Necessária

O dashboard espera os seguintes arquivos na estrutura de pastas:

```
data/
├── raw/
│   └── VIOLBR-PARQUET/
│       ├── VIOLBR19.parquet
│       ├── VIOLBR20.parquet
│       ├── VIOLBR21.parquet
│       ├── VIOLBR22.parquet
│       ├── VIOLBR23.parquet
│       └── VIOLBR24.parquet
└── config/
    └── TAB_SINANONLINE/
        └── (arquivos .cnv para decodificação)
```

## 📈 Interpretação dos Resultados

### Para Cada Hipótese

1. **Visualize o gráfico correspondente** na seção do dashboard
2. **Leia a análise textual** que acompanha cada visualização
3. **Use os filtros** para explorar diferentes segmentos
4. **Observe os KPIs** que fornecem métricas específicas para cada hipótese

### Exemplos de Uso

**Exemplo 1: Validar H7 (Contribuição de Imperatriz)**

1. Selecione "Maranhão" no filtro de UF (ou deixe "Todos" para ver a comparação)
2. Navegue até a seção "Distribuição Geográfica por Município do Maranhão (H7)"
3. Observe o gráfico de barras horizontais
4. Verifique o KPI "Contribuição de Imperatriz" que mostra o percentual e posição
5. A hipótese é validada se Imperatriz representar mais de 15% das notificações do MA

**Exemplo 2: Analisar H9 (Subnotificação Psicológica)**

1. Navegue até a seção "Composição Anual por Tipo de Violência (H9)"
2. Observe o gráfico comparativo de proporções
3. Leia a análise textual que compara as proporções
4. Se a violência psicológica tiver proporção significativamente menor, há indício de subnotificação

**Exemplo 3: Verificar H10 (Impacto da Pandemia)**

1. Navegue até a seção "Tendência de Notificações ao Longo dos Anos (H1/H10)"
2. Observe a área sombreada que destaca 2020-2021
3. Verifique se há queda em 2020 e aumento em 2021
4. Analise o gráfico mensal para detalhes do período

## 🔒 Conformidade e Privacidade

### LGPD (Lei Geral de Proteção de Dados)

- ✅ **Dados Agregados**: Apenas dados agregados são exibidos (nunca dados individuais)
- ✅ **Anonimização**: Todos os dados são anonimizados antes da análise
- ✅ **Fins Estatísticos**: Uso exclusivo para análise estatística e pesquisa
- ✅ **Sem Identificação**: Impossível identificar indivíduos através dos dados exibidos

### Limitações dos Dados

- Apenas notificações de violência contra **crianças e adolescentes (0-17 anos)** são analisadas
- Dados do período **2019-2024** (conforme disponibilidade)
- Análise baseada **exclusivamente em dados do SINAN** (sem cruzamento com IBGE)

## ⚡ Performance

- **Cache Inteligente**: O dashboard usa cache do Streamlit para otimizar carregamentos
- **Processamento Otimizado**: Suporte a DuckDB para queries rápidas em grandes volumes
- **Primeira Execução**: Pode levar alguns minutos para processar os dados pela primeira vez
- **Execuções Subsequentes**: Muito mais rápidas devido ao cache

## 🛠️ Tecnologias Utilizadas

- **Streamlit**: Framework para criação da interface web interativa
- **Pandas**: Manipulação e análise de dados
- **Plotly**: Visualizações interativas e responsivas
- **DuckDB**: Motor de consulta otimizado para grandes volumes (opcional)
- **PyArrow**: Leitura eficiente de arquivos Parquet

## 📝 Notas Importantes

1. **Dados Reais**: O dashboard utiliza dados reais do SINAN, não dados simulados
2. **Atualização**: Os dados são carregados dos arquivos Parquet disponíveis
3. **Filtros Dinâmicos**: Todos os filtros são aplicados em tempo real
4. **Visualizações Responsivas**: Os gráficos se adaptam ao tamanho da tela

## 🐛 Troubleshooting

### Erro ao Carregar Dados

- Verifique se os arquivos parquet estão na pasta `data/raw/VIOLBR-PARQUET/`
- Verifique se os arquivos não estão corrompidos
- Tente limpar o cache: Menu → Settings → Clear cache

### Gráficos Vazios

- Verifique se os filtros selecionados não estão muito restritivos
- Verifique se há dados para o período/região selecionada
- Tente remover alguns filtros para ampliar a busca

### Performance Lenta

- O primeiro carregamento pode ser lento devido ao processamento
- O cache do Streamlit acelera carregamentos subsequentes
- Considere pré-processar os dados (ver README.md principal)

## 📚 Documentação Adicional

- [Estrutura do Projeto](ESTRUTURA_PROJETO.md): Detalhes sobre a organização do código
- [Análise Técnica](ANALISE_TECNICA.md): Análise detalhada das tecnologias e arquitetura
- [README Principal](../README.md): Guia geral do projeto

---

**Desenvolvido para análise científica e pesquisa sobre violência contra crianças e adolescentes**
