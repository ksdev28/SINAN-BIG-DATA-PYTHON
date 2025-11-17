# 🚂 Guia de Deploy no Railway

Este guia explica como fazer deploy do Dashboard SINAN no Railway.

## 📋 Pré-requisitos

1. Conta no Railway (https://railway.app)
2. Dados brutos disponíveis (arquivos Parquet em `data/raw/VIOLBR-PARQUET/`)
3. Arquivos de configuração (.cnv) em `data/config/TAB_SINANONLINE/`

## 🚀 Passo a Passo

### Opção 1: Via Interface Web do Railway

1. **Acesse o Railway Dashboard**: https://railway.app/dashboard
2. **Crie um novo projeto**: Clique em "New Project"
3. **Conecte seu repositório Git**:
   - Selecione "Deploy from GitHub repo"
   - Escolha o repositório do projeto
   - O Railway detectará automaticamente o `railway.json`
4. **Configure o serviço**:
   - O Railway usará automaticamente o `start_railway.py` como comando de inicialização
   - A porta será configurada automaticamente via variável de ambiente `PORT`

### Opção 2: Via CLI do Railway

1. **Instale o Railway CLI**:
   ```bash
   npm i -g @railway/cli
   ```

2. **Faça login**:
   ```bash
   railway login
   ```

3. **Inicialize o projeto**:
   ```bash
   railway init
   ```

4. **Faça o deploy**:
   ```bash
   railway up
   ```

## ⚙️ Como Funciona

O script `start_railway.py` executa automaticamente:

1. **Verifica dados pré-processados**: Se `data/processed/sinan_data_processed.parquet` existe
2. **Executa preprocessamento** (se necessário): Roda `scripts/preprocess_data.py --sem-filtro-violencia`
3. **Inicia o dashboard**: Executa o Streamlit na porta configurada pelo Railway

## 📦 Estrutura de Arquivos

```
.
├── railway.json          # Configuração do Railway
├── start_railway.py      # Script de inicialização
├── .railwayignore        # Arquivos ignorados no deploy
└── requirements.txt      # Dependências Python
```

## 🔧 Configurações Importantes

### Variáveis de Ambiente

O Railway configura automaticamente:
- `PORT`: Porta onde o serviço será exposto (gerenciado pelo Railway)

### Recursos Necessários

O projeto precisa de:
- **Memória**: Mínimo 2GB (recomendado 4GB+ para processar grandes volumes)
- **Disco**: ~500MB para dados processados
- **CPU**: 2+ cores recomendado

### Tempo de Build

- **Primeira execução**: 5-15 minutos (depende do volume de dados)
  - Instalação de dependências: ~2-3 minutos
  - Preprocessamento: 3-12 minutos (depende dos dados)
- **Execuções subsequentes**: 1-2 minutos (usa dados pré-processados)

## 📝 Notas Importantes

1. **Dados no Repositório**: 
   - Se os dados brutos são grandes (>100MB), considere usar Railway Volumes
   - Ou faça upload dos dados pré-processados diretamente

2. **Cache do Streamlit**:
   - O Streamlit usa cache para acelerar carregamentos
   - O cache é mantido entre reinicializações

3. **Logs**:
   - Acompanhe os logs no Railway Dashboard
   - O script imprime mensagens detalhadas sobre o processo

## 🐛 Troubleshooting

### Erro: "Dados pré-processados não encontrados"
- Verifique se os dados brutos estão em `data/raw/VIOLBR-PARQUET/`
- Verifique os logs para erros no preprocessamento

### Erro: "Porta já em uso"
- O Railway gerencia a porta automaticamente
- Não é necessário configurar manualmente

### Erro: "Memória insuficiente"
- Aumente os recursos do serviço no Railway Dashboard
- Considere processar os dados localmente e fazer upload do arquivo processado

### Dashboard não carrega
- Verifique os logs no Railway Dashboard
- Confirme que o preprocessamento foi concluído com sucesso
- Verifique se a porta está configurada corretamente

## 🔗 Links Úteis

- [Documentação do Railway](https://docs.railway.app)
- [Railway Dashboard](https://railway.app/dashboard)
- [Railway CLI](https://docs.railway.app/develop/cli)

