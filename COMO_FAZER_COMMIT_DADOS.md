# 📦 Como Fazer Commit dos Dados Processados

## ✅ Solução para Problemas de Memória no Railway

Subir os dados processados para o Git resolve o problema de memória porque:
- ✅ **Dados processados são pequenos**: ~38MB (vs dados brutos que são muito maiores)
- ✅ **Não precisa processar**: O Railway usa os dados diretamente
- ✅ **Não precisa dos dados brutos**: Se tiver os processados, está tudo certo
- ✅ **Inicialização rápida**: Dashboard inicia em segundos

## 🚀 Passo a Passo

### 1. Verificar se os dados processados existem

```bash
# Verificar se o arquivo existe
ls -lh data/processed/sinan_data_processed.parquet
```

### 2. Forçar o Git a incluir os dados processados

Como o `.gitignore` estava ignorando esses arquivos, você precisa forçar:

```bash
# Adicionar os dados processados ao Git (forçando)
git add -f data/processed/sinan_data_processed.parquet
git add -f data/processed/metadata.json

# Verificar o que será commitado
git status
```

### 3. Fazer commit

```bash
git commit -m "Adiciona dados processados para deploy no Railway"
```

### 4. Fazer push

```bash
git push
```

## 📊 Tamanho dos Arquivos

- `sinan_data_processed.parquet`: ~38MB (aceitável para Git)
- `metadata.json`: ~10KB (muito pequeno)

**Total**: ~38MB (GitHub aceita arquivos até 100MB)

## ⚠️ Importante

- Os dados brutos (`data/raw/VIOLBR-PARQUET/`) **NÃO precisam** estar no Git
- O `.gitignore` ainda ignora os dados brutos (correto)
- Apenas os dados processados serão commitados

## 🔄 Após o Commit

Quando você fizer deploy no Railway:
1. O Railway baixará o repositório com os dados processados
2. O `start_railway.py` detectará que os dados já existem
3. **NÃO tentará processar** (evita erro de memória)
4. Iniciará o dashboard diretamente

## ✅ Verificação

Após o commit, verifique se os arquivos estão no Git:

```bash
git ls-files | grep "data/processed"
```

Deve mostrar:
- `data/processed/sinan_data_processed.parquet`
- `data/processed/metadata.json`

