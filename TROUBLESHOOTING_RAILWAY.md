# 🔧 Troubleshooting - Erro 502 no Railway

## ❌ Problema: Erro 502 Bad Gateway

O erro 502 geralmente indica que o Railway não consegue se conectar ao serviço Streamlit.

## 🔍 Possíveis Causas e Soluções

### 1. **Falta de Memória (Mais Comum)**

**Sintoma**: Serviço crasha durante inicialização ou preprocessamento

**Solução**:
- ✅ **FAÇA COMMIT DOS DADOS PROCESSADOS** no Git
- Isso evita o preprocessamento que consome muita memória
- Veja `COMO_FAZER_COMMIT_DADOS.md` para instruções

**Como verificar**:
- Acesse os logs no Railway Dashboard
- Procure por mensagens como "MemoryError" ou "Killed"

### 2. **Porta Não Configurada Corretamente**

**Sintoma**: Streamlit não inicia ou não escuta na porta correta

**Solução**:
- O Railway define automaticamente a variável `PORT`
- O script `start_railway.py` já está configurado para usar essa porta
- Verifique os logs para ver se a porta está sendo lida corretamente

**Como verificar nos logs**:
```
[INFO] Porta: 12345
[INFO] Variáveis de ambiente: PORT=12345
```

### 3. **Streamlit Não Está Escutando em 0.0.0.0**

**Sintoma**: Streamlit inicia mas não aceita conexões externas

**Solução**:
- O script já está configurado com `--server.address 0.0.0.0`
- O arquivo `.streamlit/config.toml` também está configurado

### 4. **Dados Não Encontrados**

**Sintoma**: Erro ao carregar dados processados

**Solução**:
- Verifique se `data/processed/sinan_data_processed.parquet` está no Git
- Execute: `git ls-files | grep "data/processed"`

### 5. **Dependências Não Instaladas**

**Sintoma**: Erro de importação de módulos

**Solução**:
- Verifique se `requirements.txt` está completo
- Os logs devem mostrar a instalação das dependências

## 📋 Checklist de Verificação

Antes de fazer deploy, verifique:

- [ ] Dados processados estão no Git (`data/processed/sinan_data_processed.parquet`)
- [ ] Arquivo `railway.json` existe e está correto
- [ ] Arquivo `start_railway.py` existe
- [ ] Arquivo `.streamlit/config.toml` existe
- [ ] `requirements.txt` está atualizado

## 🔍 Como Verificar os Logs no Railway

1. Acesse o Railway Dashboard
2. Selecione seu projeto
3. Clique em "Deployments"
4. Clique no deployment mais recente
5. Veja os logs em tempo real

**Logs esperados (sucesso)**:
```
[OK] Dados pré-processados encontrados!
[OK] Usando dados do Git. Preprocessamento não necessário.
[INFO] Porta: 12345
[OK] Streamlit iniciado com PID: 123
[OK] Dashboard disponível em: http://0.0.0.0:12345
```

**Logs de erro (memória)**:
```
[ERRO] Erro ao executar preprocessamento: ...
[ERRO] Provavelmente falta de memória.
```

## 🚀 Solução Rápida

1. **Faça commit dos dados processados**:
   ```bash
   git add -f data/processed/sinan_data_processed.parquet
   git add -f data/processed/metadata.json
   git commit -m "Adiciona dados processados"
   git push
   ```

2. **Faça redeploy no Railway**:
   - No Railway Dashboard, clique em "Redeploy"
   - Ou faça um novo commit para trigger automático

3. **Aguarde os logs mostrarem**:
   ```
   [OK] Dados pré-processados encontrados!
   [OK] Usando dados do Git. Preprocessamento não necessário.
   ```

4. **Verifique se o Streamlit iniciou**:
   ```
   [OK] Streamlit iniciado com PID: ...
   ```

## 📞 Se Ainda Não Funcionar

1. **Verifique os logs completos** no Railway Dashboard
2. **Copie os logs de erro** e verifique:
   - Mensagens de erro específicas
   - Linha onde o erro ocorreu
   - Stack trace completo

3. **Verifique recursos do Railway**:
   - Memória disponível (mínimo 2GB recomendado)
   - CPU disponível
   - Disco disponível

4. **Teste localmente primeiro**:
   ```bash
   python start_railway.py
   ```
   Isso ajuda a identificar problemas antes do deploy

