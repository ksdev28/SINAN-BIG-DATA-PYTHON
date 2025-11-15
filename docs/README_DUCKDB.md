# 🚀 Otimização com DuckDB

## O que é DuckDB?

DuckDB é um banco de dados analítico em memória projetado para processar consultas complexas de forma eficiente, mesmo em grandes volumes de dados. Ele permite executar operações SQL diretamente em arquivos Parquet sem carregar tudo na memória.

## Benefícios para o Dashboard SINAN

### ⚡ Performance
- **Consultas SQL diretas** nos arquivos parquet (sem carregar tudo)
- **Filtragem no banco** antes de carregar no pandas
- **Agregações otimizadas** (GROUP BY, COUNT, SUM, etc.)
- **Redução de memória** - carrega apenas dados necessários

### 📊 Vantagens Específicas
1. **Carregamento mais rápido**: Filtra por idade e violência diretamente no SQL
2. **Menor uso de memória**: Não precisa carregar todos os milhões de registros
3. **Consultas eficientes**: SQL otimizado para grandes volumes
4. **Compatibilidade**: Funciona com pandas - retorna DataFrames

## Como Instalar

```bash
pip install duckdb
```

Ou usando o arquivo de requisitos:
```bash
pip install -r requirements_duckdb.txt
```

## Como Funciona

O dashboard detecta automaticamente se DuckDB está instalado:

- ✅ **Com DuckDB**: Usa consultas SQL otimizadas
- ⚠️ **Sem DuckDB**: Usa método tradicional (pandas) - funciona normalmente

## Exemplo de Uso

O código já está integrado! Quando você executar:

```bash
streamlit run dashboard_sinan_real_data.py
```

O sistema automaticamente:
1. Detecta se DuckDB está disponível
2. Se sim, usa consultas SQL otimizadas
3. Se não, usa o método tradicional (fallback)

## Comparação de Performance

### Método Tradicional (Pandas)
- Carrega todos os arquivos parquet na memória
- Filtra depois de carregar tudo
- Mais lento e consome mais memória

### Método DuckDB
- Consulta SQL direta nos arquivos
- Filtra antes de carregar
- Carrega apenas dados necessários
- **Muito mais rápido e eficiente**

## Arquivos Modificados

1. **`sinan_data_processor_duckdb.py`**: Novo processador usando DuckDB
2. **`dashboard_sinan_real_data.py`**: Integração automática com fallback
3. **`requirements_duckdb.txt`**: Dependência opcional

## Notas Técnicas

- DuckDB é **opcional** - o dashboard funciona sem ele
- Se não estiver instalado, mostra uma dica mas continua funcionando
- Consultas SQL são construídas dinamicamente baseadas nos filtros
- Compatível com todos os arquivos parquet do SINAN

## Referências

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Python API](https://duckdb.org/docs/api/python)



