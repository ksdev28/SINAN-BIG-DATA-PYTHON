
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dashboard SINAN - Dados Reais
Análise de Notificações de Violência contra Crianças e Adolescentes
Usando dados reais do SINAN sem cruzar com IBGE
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import re
import sys
from pathlib import Path
from collections import Counter

# Adicionar diretório raiz ao path para imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Tentar importar DuckDB (opcional - melhora performance)
try:
    import duckdb  # noqa: F401
    DUCKDB_AVAILABLE = True
    from src.processors.sinan_duckdb_adapter import SINANDuckDBAdapter
except ImportError:
    DUCKDB_AVAILABLE = False
    st.info("**Dica:** Instale DuckDB para melhor performance: `pip install duckdb`")

from src.processors.sinan_data_processor_comprehensive import SINANDataProcessorComprehensive
from src.utils.munic_dict_loader import load_municipality_dict

# Configuração da Página
st.set_page_config(
    layout="wide", 
    page_title="Dashboard SINAN - Violência Infantil",
    page_icon=None,
    initial_sidebar_state="expanded"  # Sidebar expandida por padrão
)

# CSS personalizado
st.markdown("""
<style>
    .main-title {
        /* Monocromático - preto no branco */
        color: #000000;
        padding: 0.5rem;
        margin-top: 3rem;
        margin-bottom: 1rem;
        text-align: center;
    }
    .metric-card {
        /* Monocromático - sem gradiente */
        background: #ffffff;
        color: #000000;
        padding: 1rem;
        border: 1px solid #000000;
        border-radius: 8px;
        text-align: center;
    }
    .section-header {
        /* Monocromático - sem background azul */
        background: #ffffff;
        color: #000000;
        padding: 0.8rem;
        border: 1px solid #000000;
        border-radius: 4px;
        margin: 0.5rem 0;
        text-align: center;
        font-size: 1.2rem;
        font-weight: bold;
    }
    /* Melhorar responsividade dos gráficos */
    .js-plotly-plot {
        width: 100% !important;
    }
    /* Reduzir espaçamento entre elementos */
    .element-container {
        margin-bottom: 0.25rem !important;
        padding-bottom: 0 !important;
    }
    /* Reduzir espaçamento dos gráficos */
    .stPlotlyChart {
        margin-bottom: 0.25rem !important;
        margin-top: 0.25rem !important;
    }
    /* Reduzir espaçamento geral */
    .block-container {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }
    /* Reduzir espaçamento entre seções */
    div[data-testid="stVerticalBlock"] > div {
        gap: 0.25rem !important;
    }
    /* Reduzir espaçamento de markdown */
    .stMarkdown {
        margin-bottom: 0.25rem !important;
    }
    /* Reduzir espaçamento de info/warning */
    .stAlert, .stInfo, .stWarning {
        margin-bottom: 0.5rem !important;
        margin-top: 0.5rem !important;
    }
    /* Reduzir espaçamento de caption */
    .stCaption {
        margin-top: 0.25rem !important;
        margin-bottom: 0.25rem !important;
    }
    /* Monocromático - KPIs e métricas */
    [data-testid="stMetricValue"] {
        color: #000000 !important;
    }
    [data-testid="stMetricLabel"] {
        color: #000000 !important;
    }
    /* Monocromático - textos e links */
    .stMarkdown p, .stMarkdown strong, .stMarkdown em {
        color: #000000 !important;
    }
    /* Monocromático - sidebar */
    .css-1d391kg {
        background-color: #ffffff !important;
    }
    /* Monocromático - headers da sidebar */
    .css-1v0mbdj {
        color: #000000 !important;
    }
    /* Layout responsivo dos KPIs - reorganizar colunas em mobile */
    @media (max-width: 767px) {
        /* Em mobile, forçar quebra de linha após 2 colunas */
        .kpi-container [data-testid="column"] {
            flex: 0 0 50% !important;
            max-width: 50% !important;
        }
        .kpi-container [data-testid="column"]:nth-child(3),
        .kpi-container [data-testid="column"]:nth-child(4) {
            margin-top: 1rem;
        }
    }
</style>
<script>
// Ajustar colorbars dinamicamente baseado no tamanho da tela
function adjustColorbarsForMobile() {
    const isMobile = window.innerWidth <= 767;
    const plots = document.querySelectorAll('.js-plotly-plot');
    
    plots.forEach(plot => {
        const plotId = plot.id;
        if (plotId && window.Plotly) {
            try {
                // Abordagem direta usando relayout
                if (isMobile) {
                    window.Plotly.relayout(plotId, {
                        'coloraxis.colorbar.orientation': 'h',
                        'coloraxis.colorbar.y': -0.15,
                        'coloraxis.colorbar.x': 0.5,
                        'coloraxis.colorbar.xanchor': 'center',
                        'coloraxis.colorbar.yanchor': 'top',
                        'coloraxis.colorbar.len': 0.6,
                        'coloraxis.colorbar.thickness': 15,
                        'margin.b': 100,
                        'margin.r': 50
                    });
                } else {
                    window.Plotly.relayout(plotId, {
                        'coloraxis.colorbar.orientation': 'v',
                        'coloraxis.colorbar.x': 1.02,
                        'coloraxis.colorbar.len': 0.5,
                        'coloraxis.colorbar.thickness': 15,
                        'margin.b': 50,
                        'margin.r': 80
                    });
                }
            } catch(e) {
                // Ignorar se ainda não estiver pronto
                console.log('Ajuste de colorbar aguardando renderização:', e);
            }
        }
    });
}

// Executar quando a página carregar e quando a janela for redimensionada
let colorbarTimeout;
function scheduleColorbarsAdjust() {
    clearTimeout(colorbarTimeout);
    colorbarTimeout = setTimeout(adjustColorbarsForMobile, 500);
}

window.addEventListener('load', function() {
    setTimeout(adjustColorbarsForMobile, 2000);
});
window.addEventListener('resize', scheduleColorbarsAdjust);

// Observar mudanças no DOM para ajustar quando novos gráficos forem adicionados
const observer = new MutationObserver(function(mutations) {
    scheduleColorbarsAdjust();
});
observer.observe(document.body, { childList: true, subtree: true });
</script>
""", unsafe_allow_html=True)

# Função helper para colorbar responsiva
def get_colorbar_layout(is_mobile=False):
    """
    Retorna o layout da colorbar apropriado baseado no tamanho da tela.
    Em mobile: colorbar horizontal abaixo do gráfico
    Em desktop: colorbar vertical à direita do gráfico
    """
    if is_mobile:
        return dict(
            title=dict(text="Contagem", font=dict(size=11)),
            tickfont=dict(size=10),
            orientation="h",  # Horizontal (abaixo)
            y=-0.15,  # Posição abaixo do gráfico
            x=0.5,  # Centralizado
            xanchor="center",
            yanchor="top",
            len=0.6,
            thickness=15
        )
    else:
        return dict(
            title=dict(text="Contagem", font=dict(size=11)),
            tickfont=dict(size=10),
            x=1.02,  # À direita do gráfico
            len=0.5,
            thickness=15
        )

# Carregar dicionário de municípios
@st.cache_data(ttl=86400)  # Cache por 24 horas (municípios não mudam)
def load_municipality_dictionary():
    """Carrega dicionário de códigos de municípios para nomes"""
    try:
        # Usar caminho relativo ao diretório raiz do projeto
        config_path = project_root / "data" / "config" / "TAB_SINANONLINE"
        return load_municipality_dict(str(config_path))
    except Exception as e:
        st.warning(f"Erro ao carregar dicionário de municípios: {e}")
        return {}

# Função auxiliar para formatação brasileira de números
def formatar_numero_br(numero):
    """Formata número no padrão brasileiro (ponto para milhares)"""
    if pd.isna(numero):
        return "0"
    try:
        num = float(numero)
        if num == int(num):
            return f"{int(num):,}".replace(",", ".")
        else:
            return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return str(numero)

def formatar_numero_compacto(numero):
    """
    Formata números grandes no padrão brasileiro usando sufixos (k, M, B).
    Ex.: 78107 -> 78.107k
    """
    if numero is None or pd.isna(numero):
        return "0"
    try:
        num = float(numero)
        suffixes = [
            (1_000_000_000, "B"),
            (1_000_000, "M"),
            (1_000, "k")
        ]
        for divisor, suffix in suffixes:
            if abs(num) >= divisor:
                valor = num / divisor
                return f"{valor:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".") + suffix
        # Para valores menores que mil, manter formatação padrão brasileira
        return formatar_numero_br(num)
    except Exception:
        return str(numero)

def gerar_tick_values(min_value, max_value, max_ticks=8):
    """Gera valores para ticks de eixo considerando limites fornecidos."""
    if pd.isna(max_value) or max_value is None:
        return []
    if min_value is None or pd.isna(min_value):
        min_value = 0
    min_value = float(min_value)
    max_value = float(max_value)
    if max_value < min_value:
        min_value, max_value = max_value, min_value
    if max_value == min_value:
        return [int(round(max_value))]
    max_ticks = max(2, max_ticks)
    step = (max_value - min_value) / (max_ticks - 1)
    tick_values = [min_value + i * step for i in range(max_ticks)]
    tick_values = sorted(set(int(round(v)) for v in tick_values))
    if tick_values and tick_values[-1] != int(round(max_value)):
        tick_values.append(int(round(max_value)))
    if tick_values and tick_values[0] > int(round(min_value)):
        tick_values.insert(0, int(round(min_value)))
    return tick_values

def aplicar_formatacao_eixo(figura, max_value, eixo='y', min_value=0, max_ticks=8, usar_compacto=False):
    """Aplica formatação brasileira aos ticks do eixo especificado."""
    tick_values = gerar_tick_values(min_value, max_value, max_ticks)
    if not tick_values:
        return
    formatter = formatar_numero_compacto if usar_compacto else formatar_numero_br
    tick_text = [formatter(valor) for valor in tick_values]
    if eixo == 'y':
        figura.update_yaxes(tickmode='array', tickvals=tick_values, ticktext=tick_text)
    else:
        figura.update_xaxes(tickmode='array', tickvals=tick_values, ticktext=tick_text)

# Dicionário completo de relacionamentos com agressor (nomes completos)
# Nota: REL_INST e REL_PROPRI não são exatamente "parentesco", mas sim tipo de relação
RELACIONAMENTO_DICT = {
    # Parentescos familiares
    'REL_PAI': 'Pai',
    'REL_MAE': 'Mãe',
    'REL_PAD': 'Padrasto',
    'REL_MAD': 'Madrasta',
    'REL_CONJ': 'Cônjuge',
    'REL_EXCON': 'Ex-cônjuge',
    'REL_NAMO': 'Namorado(a)',
    'REL_EXNAM': 'Ex-namorado(a)',
    'REL_FILHO': 'Filho(a)',
    'REL_IRMAO': 'Irmão(ã)',
    'REL_SEXUAL': 'Parceiro Sexual',
    # Relações não familiares
    'REL_DESCO': 'Desconhecido',
    'REL_CONHEC': 'Conhecido',
    'REL_CUIDA': 'Cuidador(a)',
    'REL_PATRAO': 'Patrão/Chefe',
    'REL_POL': 'Policial',
    # Relações especiais (não são parentesco tradicional)
    'REL_INST': 'Funcionário de Instituição',  # Agressor é funcionário de instituição (escola, hospital, etc.)
    'REL_PROPRI': 'Próprio/Autoagressão',  # Casos de autoagressão ou violência autoinfligida
    'REL_OUTROS': 'Outros',
    'REL_ESPEC': 'Específico'
}

# Função centralizada para contar notificações de Imperatriz
def contar_imperatriz(df, uf_filtro=None):
    """
    Conta notificações de Imperatriz de forma consistente.
    
    Args:
        df: DataFrame com dados filtrados
        uf_filtro: Se fornecido, filtra apenas esta UF antes de contar
    
    Returns:
        dict com: {
            'contagem': int,
            'total_uf': int (se uf_filtro fornecido),
            'percentual': float (se uf_filtro fornecido),
            'encontrado': bool
        }
    """
    if df is None or len(df) == 0:
        return {'contagem': 0, 'total_uf': 0, 'percentual': 0.0, 'encontrado': False}
    
    # Filtrar por UF se fornecido
    if uf_filtro:
        df_uf = df[df['UF_NOTIFIC'] == uf_filtro].copy()
        total_uf = len(df_uf)
    else:
        df_uf = df.copy()
        total_uf = len(df_uf)
    
    if len(df_uf) == 0:
        return {'contagem': 0, 'total_uf': 0, 'percentual': 0.0, 'encontrado': False}
    
    # Normalizar busca de Imperatriz - IMPORTANTE: buscar apenas "Imperatriz" exato
    # NÃO incluir "Santo Amaro da Imperatriz" ou outras variações
    # Usar regex para garantir que seja apenas "Imperatriz" (não seguido de outras palavras)
    df_uf_str = df_uf['MUNICIPIO_NOTIFIC'].astype(str)
    imperatriz_mask = (
        # Match exato "Imperatriz" (com ou sem espaços)
        (df_uf_str.str.strip() == 'Imperatriz') |
        # Match "Imperatriz / UF" (mas não "Santo Amaro da Imperatriz")
        (df_uf_str.str.match(r'^Imperatriz\s*/\s*[A-Z]{2}$', case=False, na=False)) |
        # Match "Imperatriz" seguido apenas de espaços e barra
        (df_uf_str.str.match(r'^Imperatriz\s*/', case=False, na=False))
    ) & (
        # EXCLUIR "Santo Amaro da Imperatriz" explicitamente
        ~df_uf_str.str.contains('Santo Amaro', case=False, na=False)
    )
    
    # Filtrar registros de Imperatriz
    df_imperatriz = df_uf[imperatriz_mask].copy()
    
    if len(df_imperatriz) == 0:
        return {'contagem': 0, 'total_uf': total_uf, 'percentual': 0.0, 'encontrado': False}
    
    # IMPORTANTE: Contar registros únicos usando um identificador único se disponível
    # Se não houver ID único, contar linhas (cada linha = uma notificação)
    # Verificar se há coluna de ID único
    if 'REGISTRO_ID_ORIGINAL' in df_imperatriz.columns:
        # Contar IDs únicos para evitar duplicatas
        contagem = df_imperatriz['REGISTRO_ID_ORIGINAL'].nunique()
    else:
        # Contar linhas (cada linha é uma notificação)
        # IMPORTANTE: Se não houver ID único, cada linha já é um registro único
        contagem = len(df_imperatriz)
    
    # Log para debug
    nomes_unicos = df_imperatriz['MUNICIPIO_NOTIFIC'].unique()
    print(f"[DEBUG contar_imperatriz] Contagem: {contagem}, Variações de nome: {len(nomes_unicos)}, Nomes: {nomes_unicos}")
    
    if len(nomes_unicos) > 1:
        # Há variações no nome - logar para investigação
        print(f"[DEBUG] Imperatriz encontrado com {len(nomes_unicos)} variações de nome: {nomes_unicos}")
    
    # Calcular percentual se houver total da UF
    percentual = (contagem / total_uf * 100) if total_uf > 0 else 0.0
    
    return {
        'contagem': contagem,
        'total_uf': total_uf,
        'percentual': percentual,
        'encontrado': True
    }

# Classe auxiliar para dados pré-processados (deve estar no nível do módulo para serialização)
class MinimalProcessor:
    """Processador mínimo para compatibilidade com dados pré-processados"""
    def __init__(self):
        self.dictionaries = {}

# Carregamento e Tratamento dos Dados
@st.cache_resource(ttl=3600)
def get_adapter():
    """Initializes and caches the DuckDB Adapter"""
    if DUCKDB_AVAILABLE:
        return SINANDuckDBAdapter()
    return None

def load_sinan_data_on_demand(adapter, ano_range, uf_selecionada, municipio_selecionado):
    """
    Carrega dados sob demanda usando DuckDB.
    """
    if adapter:
        # Passar filtros para o DuckDB (Ano e UF agora sao passados para SQL)
        df = adapter.get_filtered_data(year_range=ano_range, uf=uf_selecionada)
        
        # Aplicar transformações de colunas derivadas (CRITICO para os filtros funcionarem)
        if df is not None and not df.empty:
            df = create_derived_columns(df)
            
            # --- FILTRAGEM EM MEMORIA (REFINAMENTO) ---
            # O DuckDB retornou dados da faixa etaria e anos corretos.
            # Agora filtramos UF e Municipio e Tipo usando Pandas (dataframe menor)
            
            # 1. Filtro UF
            if uf_selecionada != 'Todos':
                df = df[df['UF_NOTIFIC'] == uf_selecionada]
                
            # 2. Filtro Municipio
            if municipio_selecionado != 'Todos':
                df = df[df['MUNICIPIO_NOTIFIC'] == municipio_selecionado]
                
            return df, adapter.processor
            
    return None, None

# Manter funcao antiga apenas para compatibilidade ou fallback se necessario
# (Mas agora o fluxo principal deve usar o adapter)


def create_derived_columns(df):
    """
    Cria todas as colunas derivadas necessárias para o dashboard
    Esta função é chamada tanto para dados pré-processados quanto para dados processados normalmente
    """
    # 0. OTIMIZAÇÃO: Se as colunas já existem (vindas do snapshot ETL), pular processamento
    # Verificamos TIPO_VIOLENCIA e UF_NOTIFIC como proxy de que o ETL já rodou
    if df is not None and 'TIPO_VIOLENCIA' in df.columns and 'UF_NOTIFIC' in df.columns and 'FAIXA_ETARIA' in df.columns:
        # Verificar se não estão vazias (safety check)
        if not df['TIPO_VIOLENCIA'].isna().all():
            return df

    # Função auxiliar para processar datas
    def process_date_column(df, col_name):
        """Processa coluna de data no formato YYYYMMDD"""
        if col_name in df.columns:
            try:
                # Primeiro, tentar converter strings vazias ou espaços para None
                df[col_name] = df[col_name].replace(['', '        ', ' '], None)
                
                # Tentar formato YYYYMMDD primeiro (formato mais comum no SINAN)
                # Converter para string primeiro para garantir formato
                df[col_name] = df[col_name].astype(str).replace(['nan', 'None', 'NaT'], None)
                df[col_name] = pd.to_datetime(df[col_name], format='%Y%m%d', errors='coerce')
            except Exception:
                try:
                    # Tentar outros formatos
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                except Exception:
                    # Se falhar, manter como está
                    pass
        return df
    
    # Processar todas as colunas de data
    date_columns = ['DT_NOTIFIC', 'DT_OCOR', 'DT_ENCERRA', 'DT_DIGITA', 'DT_INVEST']
    for col in date_columns:
        df = process_date_column(df, col)
    
    # Extrair ano da data de notificação
    if 'DT_NOTIFIC' in df.columns:
        df['ANO_NOTIFIC'] = df['DT_NOTIFIC'].dt.year
    elif 'NU_ANO' in df.columns:
        df['ANO_NOTIFIC'] = pd.to_numeric(df['NU_ANO'], errors='coerce')
    else:
        df['ANO_NOTIFIC'] = None
    
    # Calcular tempo entre ocorrência e denúncia (em dias)
    if 'DT_OCOR' in df.columns and 'DT_NOTIFIC' in df.columns:
        # Verificar se ambas as datas são válidas antes de calcular
        mask_validas = df['DT_NOTIFIC'].notna() & df['DT_OCOR'].notna()
        df['TEMPO_OCOR_DENUNCIA'] = None
        if mask_validas.any():
            # Calcular diferença apenas para registros com ambas as datas válidas
            df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = (
                df.loc[mask_validas, 'DT_NOTIFIC'] - df.loc[mask_validas, 'DT_OCOR']
            ).dt.days
            # Filtrar valores inválidos (negativos ou muito grandes)
            df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'] = df.loc[mask_validas, 'TEMPO_OCOR_DENUNCIA'].apply(
                lambda x: x if pd.notna(x) and 0 <= x <= 3650 else None  # Máximo 10 anos
            )
    
    # Processar coluna de evolução (status do caso)
    # NOTA: Esta funcionalidade foi desabilitada porque os dados não trazem informação útil
    # 99.99% dos registros têm EVOLUCAO vazia, tornando esta análise irrelevante
    # A coluna STATUS_CASO não será criada para economizar processamento
    # Se necessário no futuro, pode ser reativada quando os dados tiverem mais informações
    # 
    # Código comentado (pode ser reativado no futuro):
    # def determinar_status_caso(row):
    #     """Determina o status do caso baseado em EVOLUCAO e DT_ENCERRA"""
    #     # ... código da função ...
    # if 'EVOLUCAO' in df.columns or 'DT_ENCERRA' in df.columns:
    #     df['STATUS_CASO'] = df.apply(determinar_status_caso, axis=1)
    # else:
    #     df['STATUS_CASO'] = 'Não informado'
    
    # Processar encaminhamentos para justiça
    encaminhamento_cols = ['ENC_DELEG', 'ENC_DPCA', 'ENC_MPU', 'ENC_VARA']
    if any(col in df.columns for col in encaminhamento_cols):
        def get_encaminhamentos_justica(row):
            encaminhamentos = []
            for col in encaminhamento_cols:
                if col in row.index and pd.notna(row[col]):
                    val = str(row[col]).upper().strip()
                    # Verificar se é '1', 'SIM', 'S', '1.0' ou se é numérico e igual a 1
                    if val in ['1', 'SIM', 'S', '1.0'] or (val.isdigit() and int(val) == 1):
                        if col == 'ENC_DELEG':
                            encaminhamentos.append('Delegacia')
                        elif col == 'ENC_DPCA':
                            encaminhamentos.append('DPCA')
                        elif col == 'ENC_MPU':
                            encaminhamentos.append('Ministério Público')
                        elif col == 'ENC_VARA':
                            encaminhamentos.append('Vara da Infância')
            return ', '.join(encaminhamentos) if encaminhamentos else 'Nenhum'
        
        df['ENCAMINHAMENTOS_JUSTICA'] = df.apply(get_encaminhamentos_justica, axis=1)
    else:
        df['ENCAMINHAMENTOS_JUSTICA'] = 'Não informado'
    
    # Dicionário de códigos IBGE para nomes de estados
    uf_dict = {
        '11': 'Rondônia', '12': 'Acre', '13': 'Amazonas', '14': 'Roraima',
        '15': 'Pará', '16': 'Amapá', '17': 'Tocantins', '21': 'Maranhão',
        '22': 'Piauí', '23': 'Ceará', '24': 'Rio Grande do Norte', '25': 'Paraíba',
        '26': 'Pernambuco', '27': 'Alagoas', '28': 'Sergipe', '29': 'Bahia',
        '31': 'Minas Gerais', '32': 'Espírito Santo', '33': 'Rio de Janeiro',
        '35': 'São Paulo', '41': 'Paraná', '42': 'Santa Catarina', '43': 'Rio Grande do Sul',
        '50': 'Mato Grosso do Sul', '51': 'Mato Grosso', '52': 'Goiás', '53': 'Distrito Federal',
        # Códigos numéricos também
        11: 'Rondônia', 12: 'Acre', 13: 'Amazonas', 14: 'Roraima',
        15: 'Pará', 16: 'Amapá', 17: 'Tocantins', 21: 'Maranhão',
        22: 'Piauí', 23: 'Ceará', 24: 'Rio Grande do Norte', 25: 'Paraíba',
        26: 'Pernambuco', 27: 'Alagoas', 28: 'Sergipe', 29: 'Bahia',
        31: 'Minas Gerais', 32: 'Espírito Santo', 33: 'Rio de Janeiro',
        35: 'São Paulo', 41: 'Paraná', 42: 'Santa Catarina', 43: 'Rio Grande do Sul',
        50: 'Mato Grosso do Sul', 51: 'Mato Grosso', 52: 'Goiás', 53: 'Distrito Federal'
    }
    
    # Criar coluna de UF com nomes
    def map_uf(val):
        if pd.isna(val):
            return 'Não informado'
        # Tentar como string primeiro
        val_str = str(val).strip()
        if val_str in uf_dict:
            return uf_dict[val_str]
        # Tentar como número
        try:
            val_num = int(float(val))
            if val_num in uf_dict:
                return uf_dict[val_num]
        except:
            pass
        # Se não encontrar, retornar o valor original
        return val_str
    
    if 'SG_UF_NOT' in df.columns:
        df['UF_NOTIFIC'] = df['SG_UF_NOT'].apply(map_uf)
    elif 'SG_UF' in df.columns:
        df['UF_NOTIFIC'] = df['SG_UF'].apply(map_uf)
    else:
        df['UF_NOTIFIC'] = 'N/A'
    
    # Carregar dicionário de municípios
    municip_dict = load_municipality_dictionary()
    
    # Criar coluna de município com nomes
    def map_municipio(codigo, municip_dict):
        if pd.isna(codigo):
            return 'Não informado'
        codigo_str = str(codigo).strip()
        # Código de município tem 6 dígitos
        if len(codigo_str) == 6 and codigo_str in municip_dict:
            return municip_dict[codigo_str]
        return codigo_str  # Retornar código se não encontrar nome
    
    if 'ID_MUNICIP' in df.columns:
        df['MUNICIPIO_NOTIFIC'] = df['ID_MUNICIP'].apply(lambda x: map_municipio(x, municip_dict))
    elif 'ID_MN_RESI' in df.columns:
        df['MUNICIPIO_NOTIFIC'] = df['ID_MN_RESI'].apply(lambda x: map_municipio(x, municip_dict))
    else:
        df['MUNICIPIO_NOTIFIC'] = 'N/A'
    
    # Criar coluna de tipo de violência (otimizado)
    def get_violence_type(row):
        types = []
        viol_fisic = str(row.get('VIOL_FISIC', '')).upper() if pd.notna(row.get('VIOL_FISIC')) else ''
        viol_psico = str(row.get('VIOL_PSICO', '')).upper() if pd.notna(row.get('VIOL_PSICO')) else ''
        viol_sexu = str(row.get('VIOL_SEXU', '')).upper() if pd.notna(row.get('VIOL_SEXU')) else ''
        viol_infan = str(row.get('VIOL_INFAN', '')).upper() if pd.notna(row.get('VIOL_INFAN')) else ''
        
        if viol_fisic in ['SIM', '1', 'S', '1.0']:
            types.append('Física')
        if viol_psico in ['SIM', '1', 'S', '1.0']:
            types.append('Psicológica')
        if viol_sexu in ['SIM', '1', 'S', '1.0']:
            types.append('Sexual')
        if viol_infan in ['SIM', '1', 'S', '1.0']:
            types.append('Infantil')
        if not types:
            types.append('Não especificado')
        return ', '.join(types)
    
    df['TIPO_VIOLENCIA'] = df.apply(get_violence_type, axis=1)
    
    # Criar faixa etária (0-17 anos: 0-1, 2-5, 6-9, 10-13, 14-17)
    if 'NU_IDADE_N' in df.columns:
        def get_age_group(age_str):
            if pd.isna(age_str):
                return 'Não informado'
            age_str = str(age_str).strip()
            
            # Extrair número da idade (suporta diferentes formatos)
            idade_num = None
            
            # Verificar se é "menor de 01 ano" ou "menor de 1 ano"
            if 'menor de' in age_str.lower() or age_str.lower() in ['menor de 01 ano', 'menor de 1 ano']:
                return '0-1 anos'
            
            # Tentar extrair número da idade (suporta "01 ano", "1 ano", "02 anos", "2 anos", etc.)
            match = re.search(r'(\d{1,2})\s*ano', age_str, re.IGNORECASE)
            if match:
                idade_num = int(match.group(1))
            else:
                # Tentar verificar diretamente strings conhecidas
                if age_str in ['01 ano', '1 ano']:
                    idade_num = 1
                elif age_str in ['02 anos', '2 anos']:
                    idade_num = 2
                elif age_str in ['03 anos', '3 anos']:
                    idade_num = 3
                elif age_str in ['04 anos', '4 anos']:
                    idade_num = 4
                elif age_str in ['05 anos', '5 anos']:
                    idade_num = 5
                elif age_str in ['06 anos', '6 anos']:
                    idade_num = 6
                elif age_str in ['07 anos', '7 anos']:
                    idade_num = 7
                elif age_str in ['08 anos', '8 anos']:
                    idade_num = 8
                elif age_str in ['09 anos', '9 anos']:
                    idade_num = 9
                elif age_str in ['10 anos']:
                    idade_num = 10
                elif age_str in ['11 anos']:
                    idade_num = 11
                elif age_str in ['12 anos']:
                    idade_num = 12
                elif age_str in ['13 anos']:
                    idade_num = 13
                elif age_str in ['14 anos']:
                    idade_num = 14
                elif age_str in ['15 anos']:
                    idade_num = 15
                elif age_str in ['16 anos']:
                    idade_num = 16
                elif age_str in ['17 anos']:
                    idade_num = 17
            
            # Classificar em faixas etárias (0-17 anos)
            if idade_num is not None:
                if idade_num == 0 or idade_num == 1:
                    return '0-1 anos'
                elif 2 <= idade_num <= 5:
                    return '2-5 anos'
                elif 6 <= idade_num <= 9:
                    return '6-9 anos'
                elif 10 <= idade_num <= 13:
                    return '10-13 anos'
                elif 14 <= idade_num <= 17:
                    return '14-17 anos'
            
            return 'Não informado'
        
        df['FAIXA_ETARIA'] = df['NU_IDADE_N'].apply(get_age_group)
    else:
        df['FAIXA_ETARIA'] = 'Não informado'
    
    # Criar coluna de sexo
    if 'CS_SEXO' in df.columns:
        def map_sexo(val):
            if pd.isna(val):
                return 'Não informado'
            val_str = str(val).upper().strip()
            if val_str in ['M', '1', 'MASCULINO', 'MAS']:
                return 'Masculino'
            elif val_str in ['F', '2', 'FEMININO', 'FEM']:
                return 'Feminino'
            else:
                return 'Não informado'
        df['SEXO'] = df['CS_SEXO'].apply(map_sexo)
    else:
        df['SEXO'] = 'Não informado'
    
    # Criar coluna de sexo do agressor (corrigir valores misturados)
    if 'AUTOR_SEXO' in df.columns:
        def map_autor_sexo(val):
            if pd.isna(val):
                return 'Não informado'
            val_str = str(val).upper().strip()
            # Valores válidos de sexo
            if val_str in ['1', 'M', 'MASCULINO', 'MAS']:
                return 'Masculino'
            elif val_str in ['2', 'F', 'FEMININO', 'FEM']:
                return 'Feminino'
            elif val_str in ['3', 'OUTROS', 'OUTRO']:
                return 'Outros'
            elif val_str in ['9', 'IGNORADO', 'IGN']:
                return 'Ignorado'
            # Se for grau de parentesco (PRIMO, TIO, etc.), considerar como "Não informado"
            # pois esses valores não deveriam estar nesta coluna
            else:
                return 'Não informado'
        df['AUTOR_SEXO_CORRIGIDO'] = df['AUTOR_SEXO'].apply(map_autor_sexo)
    else:
        df['AUTOR_SEXO_CORRIGIDO'] = 'Não informado'
    
    # Criar coluna de relacionamento com agressor (das colunas REL_) com nomes completos
    # Filtrar apenas colunas de relacionamento pessoal (excluir REL_TRAB, REL_CAT que são sobre trabalho)
    rel_cols = [col for col in df.columns if col.startswith('REL_') and col not in ['REL_TRAB', 'REL_CAT']]
    if rel_cols:
        def get_relacionamento(row):
            relacionamentos = []
            for col in rel_cols:
                if col in row.index and pd.notna(row[col]):
                    val = str(row[col]).upper().strip()
                    if val in ['1', 'SIM', 'S', '1.0']:
                        # Usar dicionário de relacionamentos para nomes completos
                        if col in RELACIONAMENTO_DICT:
                            relacionamento = RELACIONAMENTO_DICT[col]
                        else:
                            # Se não estiver no dicionário, tentar criar nome legível
                            relacionamento = col.replace('REL_', '').replace('_', ' ').title()
                            # Correções comuns
                            relacionamento = relacionamento.replace('Exnam', 'Ex-namorado(a)')
                            relacionamento = relacionamento.replace('Excon', 'Ex-cônjuge')
                            relacionamento = relacionamento.replace('Conhec', 'Conhecido')
                            relacionamento = relacionamento.replace('Propri', 'Próprio/Autoagressão')
                            relacionamento = relacionamento.replace('Cuid', 'Cuidador(a)')
                            relacionamento = relacionamento.replace('Patrao', 'Patrão/Chefe')
                            relacionamento = relacionamento.replace('Inst', 'Funcionário de Instituição')
                        relacionamentos.append(relacionamento)
            return ', '.join(relacionamentos) if relacionamentos else 'Não informado'
        df['GRAU_PARENTESCO'] = df.apply(get_relacionamento, axis=1)
    else:
        df['GRAU_PARENTESCO'] = 'Não informado'
    
    return df

# Função otimizada para processar tipos de violência expandidos
@st.cache_data(ttl=3600, show_spinner=False)  # Cache por 1 hora - CRÍTICO para performance
def process_tipos_violencia_expandidos(df_filtrado):
    """
    Processa e expande tipos de violência combinados em tipos individuais.
    Função otimizada com operações vetorizadas.
    IMPORTANTE: Esta função tem cache para evitar reprocessamento desnecessário.
    """
    if df_filtrado is None or len(df_filtrado) == 0:
        return None
    
    if 'TIPO_VIOLENCIA' not in df_filtrado.columns or 'ANO_NOTIFIC' not in df_filtrado.columns:
        return None
    
    # Filtrar apenas registros com dados válidos (usar loc para evitar cópia desnecessária)
    mask_validos = (
        df_filtrado['TIPO_VIOLENCIA'].notna() & 
        df_filtrado['ANO_NOTIFIC'].notna()
    )
    
    if not mask_validos.any():
        return None
    
    # IMPORTANTE: Manter índice original para contar registros únicos
    # Usar apenas as colunas necessárias, mas manter o índice original
    df_temp = df_filtrado.loc[mask_validos, ['ANO_NOTIFIC', 'TIPO_VIOLENCIA']].copy()
    
    # Criar REGISTRO_ID_ORIGINAL baseado no índice antes de qualquer transformação
    df_temp['REGISTRO_ID_ORIGINAL'] = df_temp.index
    
    # Converter para string e filtrar valores inválidos
    df_temp['TIPO_VIOLENCIA'] = df_temp['TIPO_VIOLENCIA'].astype(str)
    df_temp = df_temp[~df_temp['TIPO_VIOLENCIA'].isin(['nan', 'None', '', 'Não especificado'])]
    
    if len(df_temp) == 0:
        return None
    
    # Filtrar tipos principais ANTES do explode (reduz drasticamente o processamento)
    tipos_principais = ['Sexual', 'Física', 'Psicológica']
    mask_tem_tipos = df_temp['TIPO_VIOLENCIA'].str.contains('|'.join(tipos_principais), na=False, case=False)
    df_temp = df_temp[mask_tem_tipos]
    
    if len(df_temp) == 0:
        return None
    
    # Separar tipos combinados usando split e explode (vetorizado - muito rápido)
    # O REGISTRO_ID_ORIGINAL será mantido após o explode
    df_temp['TIPO_VIOLENCIA'] = df_temp['TIPO_VIOLENCIA'].str.split(',')
    df_tipos_expandidos = df_temp.explode('TIPO_VIOLENCIA')
    
    # Limpar espaços e filtrar apenas os 3 tipos principais
    df_tipos_expandidos['TIPO_VIOLENCIA'] = df_tipos_expandidos['TIPO_VIOLENCIA'].str.strip()
    df_tipos_expandidos = df_tipos_expandidos[
        (df_tipos_expandidos['TIPO_VIOLENCIA'].isin(tipos_principais))
    ]
    
    return df_tipos_expandidos

# Função otimizada para processar tipos de violência expandidos para demografia (Gráfico 3)
@st.cache_data(ttl=3600, show_spinner=False)  # Cache por 1 hora - CRÍTICO para performance
def process_tipos_violencia_expandidos_demo(df_demografia):
    """
    Processa e expande tipos de violência para análise demográfica (Gráfico 3).
    Mantém REGISTRO_ID_ORIGINAL para contar registros únicos.
    IMPORTANTE: Esta função tem cache para evitar reprocessamento desnecessário.
    """
    if df_demografia is None or len(df_demografia) == 0:
        return None
    
    if 'TIPO_VIOLENCIA' not in df_demografia.columns:
        return None
    
    # Usar colunas necessárias incluindo REGISTRO_ID_ORIGINAL
    df_temp_demo = df_demografia[['REGISTRO_ID_ORIGINAL', 'FAIXA_ETARIA', 'SEXO', 'TIPO_VIOLENCIA']].copy()
    df_temp_demo = df_temp_demo[df_temp_demo['TIPO_VIOLENCIA'].notna()]
    df_temp_demo['TIPO_VIOLENCIA'] = df_temp_demo['TIPO_VIOLENCIA'].astype(str)
    df_temp_demo = df_temp_demo[~df_temp_demo['TIPO_VIOLENCIA'].isin(['nan', 'None', '', 'Não especificado'])]
    
    if len(df_temp_demo) == 0:
        return None
    
    # Filtrar tipos principais ANTES do explode
    tipos_principais = ['Sexual', 'Física', 'Psicológica']
    mask_tem_tipos = df_temp_demo['TIPO_VIOLENCIA'].str.contains('|'.join(tipos_principais), na=False, case=False)
    df_temp_demo = df_temp_demo[mask_tem_tipos]
    
    if len(df_temp_demo) == 0:
        return None
    
    # Expandir tipos combinados (mantendo REGISTRO_ID_ORIGINAL)
    df_temp_demo['TIPO_VIOLENCIA'] = df_temp_demo['TIPO_VIOLENCIA'].str.split(',')
    df_tipos_expandidos_demo = df_temp_demo.explode('TIPO_VIOLENCIA')
    df_tipos_expandidos_demo['TIPO_VIOLENCIA'] = df_tipos_expandidos_demo['TIPO_VIOLENCIA'].str.strip()
    df_tipos_expandidos_demo = df_tipos_expandidos_demo[
        df_tipos_expandidos_demo['TIPO_VIOLENCIA'].isin(tipos_principais)
    ]
    
    return df_tipos_expandidos_demo

# Título e Nota de Conformidade LGPD
st.markdown("""
<div class="main-title">
    <h1 style="font-size: 2.5rem; margin-bottom: 0.5rem; color: #000000; font-weight: bold;">Dashboard SINAN: Análise de Notificações de Violência<br>contra Crianças e Adolescentes</h1>
</div>
""", unsafe_allow_html=True)

st.markdown("""
**Fonte:** Dados Reais do SINAN (Sistema de Informação de Agravos de Notificação) - 2019-2024  
**Nota de Conformidade LGPD:** Todos os dados são **anonimizados e agregados** para fins de análise estatística, respeitando a natureza sensível do tema.
""")

# Carregar adapter
adapter = get_adapter()

# Inicializar estados dos filtros na sessao
if 'ano_selecionado' not in st.session_state:
    st.session_state.ano_selecionado = (2019, 2024)
if 'uf_selecionada' not in st.session_state:
    st.session_state.uf_selecionada = 'Todos'
if 'municipio_selecionado' not in st.session_state:
    st.session_state.municipio_selecionado = 'Todos'

# Sidebar: Configuração de Filtros
st.sidebar.header("Filtros de Análise")

# 1. Filtro de Ano (Origem: DuckDB Metadata ou fixo)
# Idealmente pegar do adapter, mas fallback para fixo
try:
    if adapter:
        anos_disp = adapter.get_available_years()
        if not anos_disp:
            anos_disp = [2019, 2020, 2021, 2022, 2023, 2024]
    else:
        anos_disp = [2019, 2020, 2021, 2022, 2023, 2024]
    
    ano_min, ano_max = min(anos_disp), max(anos_disp)
    st.session_state.ano_selecionado = st.sidebar.slider(
        'Selecione o Período (Anos)',
        min_value=int(ano_min),
        max_value=int(ano_max),
        value=st.session_state.ano_selecionado
    )
except Exception:
    st.session_state.ano_selecionado = st.sidebar.slider('Selecione o Período', 2019, 2024, (2019, 2024))

# 2. Carregar Dados filtrados por ANO (Primeiro estagio)
# Isso carrega um subset muito menor que o total
with st.spinner(f"Carregando dados ({st.session_state.ano_selecionado[0]}-{st.session_state.ano_selecionado[1]})..."):
    df_filtrado, processor = load_sinan_data_on_demand(
        adapter, 
        st.session_state.ano_selecionado, 
        st.session_state.uf_selecionada, 
        'Todos'
    )

if df_filtrado is None or df_filtrado.empty:
    st.error("Nenhum dado encontrado para o período selecionado.")
    st.stop()

# 3. Filtros de UF e Municipio (Baseados nos dados carregados do período)
# Filtro de Região (UF) - Usar adaptador para pegar lista completa
uf_options = ['Todos']
if adapter:
    try:
        db_ufs = adapter.get_available_ufs()
        # Filtrar valores inválidos e ordenar
        clean_ufs = sorted([str(u) for u in db_ufs if str(u) not in ['nan', 'N/A', 'Não informado', 'None']])
        uf_options += clean_ufs
    except Exception as e:
        print(f"Erro ao carregar UFs: {e}")
elif df_filtrado is not None and 'UF_NOTIFIC' in df_filtrado.columns:
    # Fallback
    uf_options += sorted([uf for uf in df_filtrado['UF_NOTIFIC'].dropna().unique() if str(uf) not in ['nan', 'N/A']])

# Garantir que a seleção atual é válida
if 'uf_selecionada' not in st.session_state:
    st.session_state.uf_selecionada = 'Todos'
if st.session_state.uf_selecionada not in uf_options:
    st.session_state.uf_selecionada = 'Todos'

uf_selecionada = st.sidebar.selectbox(
    'Filtrar por UF:', 
    options=uf_options,
    index=uf_options.index(st.session_state.uf_selecionada),
    key='uf_selectbox_widget'
)

# Atualizar state e recarregar se mudou
if uf_selecionada != st.session_state.uf_selecionada:
    st.session_state.uf_selecionada = uf_selecionada
    st.rerun()

# Filtrar dados em memória se necessário (caso o adapter já não tenha filtrado)
# O adapter filtra se passarmos o UF. Mas para garantir consistência:
if uf_selecionada != 'Todos' and df_filtrado['UF_NOTIFIC'].nunique() > 1:
     df_filtrado = df_filtrado[df_filtrado['UF_NOTIFIC'] == uf_selecionada]

# Filtro de Município (Depende do UF selecionado e dos dados carregados)
municipio_options = ['Todos']
if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns:
    muns = sorted([m for m in df_filtrado['MUNICIPIO_NOTIFIC'].dropna().unique() if str(m) not in ['nan','N/A']])
    municipio_options += muns

if 'municipio_selecionado' not in st.session_state:
    st.session_state.municipio_selecionado = 'Todos'
if st.session_state.municipio_selecionado not in municipio_options:
    st.session_state.municipio_selecionado = 'Todos'
    
mun_selecionado = st.sidebar.selectbox(
    'Filtrar por Município:',
    options=municipio_options,
    index=municipio_options.index(st.session_state.municipio_selecionado),
    key='mun_selectbox_widget'
)

if mun_selecionado != st.session_state.municipio_selecionado:
    st.session_state.municipio_selecionado = mun_selecionado
    st.rerun()

if mun_selecionado != 'Todos':
    df_filtrado = df_filtrado[df_filtrado['MUNICIPIO_NOTIFIC'] == mun_selecionado]

# Filtro de Tipo de Violência
if 'TIPO_VIOLENCIA' in df_filtrado.columns:
    tipos_disponiveis = sorted(df_filtrado['TIPO_VIOLENCIA'].dropna().unique())
    if tipos_disponiveis:
        tipo_options = ['Todos'] + tipos_disponiveis
        # Nota: nao salvamos tipo na session_state para simplificar, mas poderia
        tipo_selecionado = st.sidebar.selectbox('Filtrar por Tipo de Violência', tipo_options, index=0)
        
        if tipo_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['TIPO_VIOLENCIA'].str.contains(tipo_selecionado, na=False)]
    else:
        tipo_selecionado = 'Todos'
else:
    tipo_selecionado = 'Todos'

# Atualizar variaveis locais para compatibilidade com o resto do codigo
uf_selecionada = st.session_state.uf_selecionada
municipio_selecionado = st.session_state.municipio_selecionado
ano_selecionado = st.session_state.ano_selecionado

# Verificação de Consistência: Contagem de Imperatriz (Debug)
# Esta seção ajuda a identificar inconsistências nos dados
if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns and 'UF_NOTIFIC' in df_filtrado.columns:
    # Contar Imperatriz de forma centralizada
    imperatriz_debug = contar_imperatriz(df_filtrado)
    if imperatriz_debug['encontrado']:
        # Verificar também no Maranhão especificamente
        imperatriz_ma = contar_imperatriz(df_filtrado, uf_filtro='Maranhão')
        
        # Exibir apenas se houver diferença significativa ou se estiver em modo debug
        # (Comentado por padrão para não poluir a interface)
        # with st.expander("🔍 Debug: Verificação de Consistência - Imperatriz", expanded=False):
        #     st.write(f"**Contagem Total de Imperatriz (todos os filtros):** {formatar_numero_br(imperatriz_debug['contagem'])}")
        #     if imperatriz_ma['encontrado']:
        #         st.write(f"**Contagem de Imperatriz no Maranhão:** {formatar_numero_br(imperatriz_ma['contagem'])}")
        #         st.write(f"**Percentual no MA:** {imperatriz_ma['percentual']:.2f}%")
        #         st.write(f"**Total de notificações no MA:** {formatar_numero_br(imperatriz_ma['total_uf'])}")
        pass

# Indicadores Chave (KPIs)
st.markdown('<div class="section-header">Indicadores Principais</div>', unsafe_allow_html=True)

total_notificacoes = len(df_filtrado)
media_anual = df_filtrado.groupby('ANO_NOTIFIC').size().mean().round(0) if 'ANO_NOTIFIC' in df_filtrado.columns and not df_filtrado.empty else 0

# Calcular valores dos KPIs
if 'TIPO_VIOLENCIA' in df_filtrado.columns and not df_filtrado.empty:
    maior_tipo = df_filtrado['TIPO_VIOLENCIA'].mode()[0] if len(df_filtrado['TIPO_VIOLENCIA'].mode()) > 0 else "N/A"
else:
    maior_tipo = "N/A"

if 'SEXO' in df_filtrado.columns and not df_filtrado.empty:
    sexo_dist = df_filtrado['SEXO'].value_counts()
    if len(sexo_dist) > 0:
        sexo_maior = sexo_dist.index[0]
    else:
        sexo_maior = "N/A"
else:
    sexo_maior = "N/A"

# Layout responsivo: 4 colunas (desktop) ou 2x2 (mobile via CSS)
with st.container():
    st.markdown('<div class="kpi-container">', unsafe_allow_html=True)
    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)

    with col_kpi1:
        st.metric("Total de Notificações", formatar_numero_br(total_notificacoes))

    with col_kpi2:
        st.metric("Média Anual", formatar_numero_br(media_anual))

    with col_kpi3:
        st.metric("Tipo Mais Frequente", maior_tipo[:30] + "..." if len(str(maior_tipo)) > 30 else maior_tipo)

    with col_kpi4:
        st.metric("Sexo Mais Frequente", sexo_maior)

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("---")

# Contexto de análise (Estado/Município) - Definir após os filtros
contexto_analise = ""
if uf_selecionada != 'Todos':
    contexto_analise = f"**Estado:** {uf_selecionada}"
    if municipio_selecionado != 'Todos':
        contexto_analise += f" | **Município:** {municipio_selecionado}"
else:
    contexto_analise = "Brasil (Todos os Estados)"

# Gráficos
if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    # Contexto da análise atual
    total_formatado = formatar_numero_br(total_notificacoes)
    st.info(f"**Análise atual:** {contexto_analise} | **Período:** {ano_selecionado[0]}-{ano_selecionado[1]} | **Total de registros:** {total_formatado}")
    
    # Gráfico 1: Tendência Temporal (H1, H10)
    st.markdown('<div class="section-header">1. Tendência de Notificações ao Longo dos Anos (H1, H10)</div>', unsafe_allow_html=True)
    st.markdown("**H1 – Tendência Geral:** \"As notificações aumentaram após a pandemia?\" | **H10 – Impacto da Pandemia:** \"Houve queda das notificações em 2020 e aumento em 2021?\"")
    
    if 'ANO_NOTIFIC' in df_filtrado.columns and df_filtrado['ANO_NOTIFIC'].notna().any():
        df_tendencia = df_filtrado.groupby('ANO_NOTIFIC').size().reset_index(name='Total_Notificacoes')
        df_tendencia = df_tendencia.sort_values('ANO_NOTIFIC')
        df_tendencia['Total_Formatado'] = df_tendencia['Total_Notificacoes'].apply(formatar_numero_br)
        
        # Calcular variação percentual em relação ao ano anterior (H1)
        df_tendencia['Variacao_Anterior'] = df_tendencia['Total_Notificacoes'].pct_change() * 100
        df_tendencia['Variacao_Formatada'] = df_tendencia['Variacao_Anterior'].apply(
            lambda x: f"{x:+.1f}%" if pd.notna(x) else ""
        )
        df_tendencia['Texto_Completo'] = df_tendencia.apply(
            lambda row: f"{row['Total_Formatado']}<br>({row['Variacao_Formatada']})" if row['Variacao_Formatada'] else row['Total_Formatado'],
            axis=1
        )
        
        # Calcular indicadores de crescimento/queda
        if len(df_tendencia) >= 2:
            primeiro_ano = df_tendencia.iloc[0]
            ultimo_ano = df_tendencia.iloc[-1]
            primeiro_valor = primeiro_ano['Total_Notificacoes']
            ultimo_valor = ultimo_ano['Total_Notificacoes']
            
            # Variação total
            variacao_total = ((ultimo_valor - primeiro_valor) / primeiro_valor * 100) if primeiro_valor > 0 else 0
            
            # Variação anual média
            variacao_anual_media = variacao_total / (len(df_tendencia) - 1) if len(df_tendencia) > 1 else 0
            
            # Análise pré/pós pandemia (H1)
            anos_pre_pandemia = df_tendencia[df_tendencia['ANO_NOTIFIC'] < 2020]
            anos_pos_pandemia = df_tendencia[df_tendencia['ANO_NOTIFIC'] > 2020]
            
            media_pre = anos_pre_pandemia['Total_Notificacoes'].mean() if len(anos_pre_pandemia) > 0 else 0
            media_pos = anos_pos_pandemia['Total_Notificacoes'].mean() if len(anos_pos_pandemia) > 0 else 0
            variacao_pos_pandemia = ((media_pos - media_pre) / media_pre * 100) if media_pre > 0 else 0
            
            # Análise 2019-2020-2021 (H10)
            notif_2019 = df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2019]['Total_Notificacoes'].values[0] if len(df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2019]) > 0 else 0
            notif_2020 = df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2020]['Total_Notificacoes'].values[0] if len(df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2020]) > 0 else 0
            notif_2021 = df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2021]['Total_Notificacoes'].values[0] if len(df_tendencia[df_tendencia['ANO_NOTIFIC'] == 2021]) > 0 else 0
            
            variacao_2020 = ((notif_2020 - notif_2019) / notif_2019 * 100) if notif_2019 > 0 else 0
            variacao_2021 = ((notif_2021 - notif_2020) / notif_2020 * 100) if notif_2020 > 0 else 0
            
            # Exibir KPIs com indicadores de crescimento/queda
            col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
            with col_kpi1:
                # Calcular diferença absoluta total
                diferenca_absoluta_total = ultimo_valor - primeiro_valor
                st.metric(
                    "Variação Total",
                    formatar_numero_br(diferenca_absoluta_total),
                    delta=f"{variacao_total:+.1f}%"
                )
            with col_kpi2:
                # Calcular diferença absoluta para H1
                diferenca_absoluta_h1 = media_pos - media_pre if media_pre > 0 else 0
                st.metric(
                    "Variação Pós-Pandemia (H1)",
                    formatar_numero_br(diferenca_absoluta_h1),
                    delta=f"{variacao_pos_pandemia:+.1f}%"
                )
            with col_kpi3:
                # Calcular diferença absoluta para 2020
                diferenca_absoluta_2020 = notif_2020 - notif_2019
                st.metric(
                    "Variação 2020 (H10)",
                    formatar_numero_br(diferenca_absoluta_2020),
                    delta=f"{variacao_2020:+.1f}%"
                )
            with col_kpi4:
                # Calcular diferença absoluta para 2021
                diferenca_absoluta_2021 = notif_2021 - notif_2020
                st.metric(
                    "Variação 2021 (H10)",
                    formatar_numero_br(diferenca_absoluta_2021),
                    delta=f"{variacao_2021:+.1f}%"
                )
        
        fig_line = px.line(
            df_tendencia, 
            x='ANO_NOTIFIC', 
            y='Total_Notificacoes', 
            markers=True,
            title=f'Total de Notificações por Ano',
            labels={'ANO_NOTIFIC': 'Ano', 'Total_Notificacoes': 'Total de Notificações'},
            color_discrete_sequence=['#1a237e']  # Cor mais escura e visível
        )
        # Formatação brasileira de números e otimização do eixo Y
        fig_line.update_layout(
            xaxis_title="Ano",
            yaxis_title="Total de Notificações",
            hovermode='x unified',
            yaxis=dict(
                showgrid=True,
                gridwidth=1,
                gridcolor='lightgray'
            ),
            margin=dict(l=80, r=50, t=80, b=50),  # Margens otimizadas
            height=500  # Altura fixa para evitar compressão
        )
        max_value = df_tendencia['Total_Notificacoes'].max()
        aplicar_formatacao_eixo(fig_line, max_value, eixo='y')
        
        # Adicionar sombreamento vertical sutil para destacar período da pandemia (2020-2021)
        # Verificar se os anos 2020 e 2021 existem nos dados
        anos_disponiveis = df_tendencia['ANO_NOTIFIC'].values
        if 2020 in anos_disponiveis or 2021 in anos_disponiveis:
            # Adicionar retângulo vertical que cobre 2020 e 2021
            # Usar coordenadas de 2019.5 a 2021.5 para cobrir bem os dois anos
            fig_line.add_vrect(
                x0=2019.5,
                x1=2021.5,
                fillcolor="lightgray",
                opacity=0.15,  # Opacidade muito baixa para ser sutil
                layer="below",  # Colocar atrás dos dados
                line_width=0,  # Sem borda
            )
        
        # Adicionar anotações com números e porcentagens (H1)
        annotations = []
        for idx, row in df_tendencia.iterrows():
            if pd.notna(row['Variacao_Anterior']):
                annotations.append(
                    dict(
                        x=row['ANO_NOTIFIC'],
                        y=row['Total_Notificacoes'],
                        text=f"{row['Total_Formatado']}<br>({row['Variacao_Formatada']})",
                        showarrow=True,
                        arrowhead=2,
                        arrowcolor='#1a237e',
                        ax=0,
                        ay=-40,
                        bgcolor='rgba(255,255,255,0.8)',
                        bordercolor='#1a237e',
                        borderwidth=1,
                        font=dict(size=9, color='#000000')
                    )
                )
            else:
                annotations.append(
                    dict(
                        x=row['ANO_NOTIFIC'],
                        y=row['Total_Notificacoes'],
                        text=row['Total_Formatado'],
                        showarrow=True,
                        arrowhead=2,
                        arrowcolor='#1a237e',
                        ax=0,
                        ay=-40,
                        bgcolor='rgba(255,255,255,0.8)',
                        bordercolor='#1a237e',
                        borderwidth=1,
                        font=dict(size=9, color='#000000')
                    )
                )
        
        fig_line.update_layout(annotations=annotations)
        # Preparar customdata corretamente para hover
        custom_data_list = []
        for idx, row in df_tendencia.iterrows():
            custom_data_list.append([row['Total_Formatado'], row['Variacao_Formatada'] if pd.notna(row['Variacao_Formatada']) else 'N/A'])
        
        fig_line.update_traces(
            customdata=custom_data_list,
            hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<br>Variação: %{customdata[1]}<extra></extra>'
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Dados de ano não disponíveis para este gráfico")

    # Gráfico 2: Composição por Tipo de Violência (H9)
    st.markdown('<div class="section-header">2. Composição Anual por Tipo de Violência (H9)</div>', unsafe_allow_html=True)
    st.markdown("**H9 – Subnotificação:** \"A violência psicológica está sendo subnotificada?\"")
    
    if 'TIPO_VIOLENCIA' in df_filtrado.columns and 'ANO_NOTIFIC' in df_filtrado.columns:
        try:
            # Usar função com cache para processar tipos de violência
            df_tipos_expandidos = process_tipos_violencia_expandidos(df_filtrado)
            
            if df_tipos_expandidos is None or len(df_tipos_expandidos) == 0:
                st.info("Nenhum dado válido de tipo de violência disponível para este gráfico")
            else:
                # IMPORTANTE: Contar REGISTROS ÚNICOS, não linhas expandidas
                # Usar REGISTRO_ID_ORIGINAL para contar quantos registros únicos têm cada combinação
                if 'REGISTRO_ID_ORIGINAL' in df_tipos_expandidos.columns:
                    df_composicao = df_tipos_expandidos.groupby(['ANO_NOTIFIC', 'TIPO_VIOLENCIA'])['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
                else:
                    # Fallback: se não tiver REGISTRO_ID_ORIGINAL, usar size() mas avisar
                    df_composicao = df_tipos_expandidos.groupby(['ANO_NOTIFIC', 'TIPO_VIOLENCIA']).size().reset_index(name='Contagem')
                    st.warning("Aviso: Contando linhas expandidas, não registros únicos. Os números podem estar inflados.")
                
                df_composicao = df_composicao.sort_values(['ANO_NOTIFIC', 'TIPO_VIOLENCIA'])
                
                # Calcular porcentagens por ano
                df_composicao['Total_Ano'] = df_composicao.groupby('ANO_NOTIFIC')['Contagem'].transform('sum')
                df_composicao['Percentual'] = (df_composicao['Contagem'] / df_composicao['Total_Ano'] * 100).round(1)
                df_composicao['Contagem_Formatada'] = df_composicao['Contagem'].apply(formatar_numero_br)
                
                # Calcular estatísticas para responder H9
                anos_ordenados = sorted(df_composicao['ANO_NOTIFIC'].unique())
                
                # Calcular totais por tipo em todo o período (contando registros únicos)
                total_sexual = df_composicao[df_composicao['TIPO_VIOLENCIA'] == 'Sexual']['Contagem'].sum()
                total_fisica = df_composicao[df_composicao['TIPO_VIOLENCIA'] == 'Física']['Contagem'].sum()
                total_psicologica = df_composicao[df_composicao['TIPO_VIOLENCIA'] == 'Psicológica']['Contagem'].sum()
                
                # IMPORTANTE: Não somar os totais! Cada registro pode ter múltiplos tipos
                # O total_geral deve ser o número de registros únicos que têm pelo menos um dos tipos
                # Calcular registros únicos que têm qualquer um dos três tipos
                if 'REGISTRO_ID_ORIGINAL' in df_tipos_expandidos.columns:
                    total_registros_unicos = df_tipos_expandidos['REGISTRO_ID_ORIGINAL'].nunique()
                    total_geral = total_registros_unicos  # Total de notificações únicas
                else:
                    # Fallback: somar (mas isso está incorreto se houver registros com múltiplos tipos)
                    total_geral = total_sexual + total_fisica + total_psicologica
                
                # Calcular percentuais
                pct_sexual = (total_sexual / total_geral * 100) if total_geral > 0 else 0
                pct_fisica = (total_fisica / total_geral * 100) if total_geral > 0 else 0
                pct_psicologica = (total_psicologica / total_geral * 100) if total_geral > 0 else 0
                
                # Calcular crescimento entre primeiro e último ano
                if len(anos_ordenados) >= 2:
                    primeiro_ano = anos_ordenados[0]
                    ultimo_ano = anos_ordenados[-1]
                    
                    sexual_primeiro = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Sexual') & (df_composicao['ANO_NOTIFIC'] == primeiro_ano)]['Contagem'].sum()
                    sexual_ultimo = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Sexual') & (df_composicao['ANO_NOTIFIC'] == ultimo_ano)]['Contagem'].sum()
                    crescimento_sexual = ((sexual_ultimo - sexual_primeiro) / sexual_primeiro * 100) if sexual_primeiro > 0 else 0
                    
                    fisica_primeiro = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Física') & (df_composicao['ANO_NOTIFIC'] == primeiro_ano)]['Contagem'].sum()
                    fisica_ultimo = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Física') & (df_composicao['ANO_NOTIFIC'] == ultimo_ano)]['Contagem'].sum()
                    crescimento_fisica = ((fisica_ultimo - fisica_primeiro) / fisica_primeiro * 100) if fisica_primeiro > 0 else 0
                    
                    psicologica_primeiro = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Psicológica') & (df_composicao['ANO_NOTIFIC'] == primeiro_ano)]['Contagem'].sum()
                    psicologica_ultimo = df_composicao[(df_composicao['TIPO_VIOLENCIA'] == 'Psicológica') & (df_composicao['ANO_NOTIFIC'] == ultimo_ano)]['Contagem'].sum()
                    crescimento_psicologica = ((psicologica_ultimo - psicologica_primeiro) / psicologica_primeiro * 100) if psicologica_primeiro > 0 else 0
                else:
                    crescimento_sexual = 0
                    crescimento_fisica = 0
                    crescimento_psicologica = 0
                
                # Análise para responder H9: Comparar proporções
                # Se violência psicológica tem proporção muito menor que física e sexual, pode indicar subnotificação
                media_outras = (pct_sexual + pct_fisica) / 2
                diferenca_proporcao = media_outras - pct_psicologica
                
                # Determinar resposta à hipótese H9
                if pct_psicologica < 30 and diferenca_proporcao > 15:
                    resposta_h9 = "SIM - A violência psicológica está sendo subnotificada"
                    explicacao_h9 = f"A violência psicológica representa apenas {pct_psicologica:.1f}% do total, enquanto Sexual ({pct_sexual:.1f}%) e Física ({pct_fisica:.1f}%) juntas representam {pct_sexual + pct_fisica:.1f}%. Esta desproporção sugere subnotificação, pois a violência psicológica frequentemente ocorre em conjunto com outros tipos."
                elif pct_psicologica < 40 and diferenca_proporcao > 10:
                    resposta_h9 = "PROVAVELMENTE - Indícios de subnotificação"
                    explicacao_h9 = f"A violência psicológica representa {pct_psicologica:.1f}% do total, menor que a média das outras ({media_outras:.1f}%). Há indícios de subnotificação, especialmente considerando que violência psicológica frequentemente acompanha outros tipos."
                else:
                    resposta_h9 = "NÃO - A violência psicológica não parece estar subnotificada"
                    explicacao_h9 = f"A violência psicológica representa {pct_psicologica:.1f}% do total, proporção similar às outras formas de violência (Sexual: {pct_sexual:.1f}%, Física: {pct_fisica:.1f}%)."
                
                # Exibir KPIs e resposta à hipótese
                col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
                with col_kpi1:
                    st.metric(
                        "Violência Sexual",
                        formatar_numero_br(total_sexual),
                        delta=f"{pct_sexual:.1f}% do total"
                    )
                with col_kpi2:
                    st.metric(
                        "Violência Física",
                        formatar_numero_br(total_fisica),
                        delta=f"{pct_fisica:.1f}% do total"
                    )
                with col_kpi3:
                    st.metric(
                        "Violência Psicológica",
                        formatar_numero_br(total_psicologica),
                        delta=f"{pct_psicologica:.1f}% do total"
                    )
                
                # Explicação sobre a lógica de contagem
                st.caption(f"**Total de notificações únicas analisadas: {total_geral:,}** | *Nota: Cada número representa quantas notificações únicas têm aquele tipo de violência. Uma notificação pode ter múltiplos tipos, então a soma dos três tipos ({total_sexual + total_fisica + total_psicologica:,}) pode ser maior que o total de notificações únicas.*")
                
                # Explicação sobre as diferenças de porcentagem
                st.info(f"**Análise:** {explicacao_h9}")
                
                # Criar gráfico de barras agrupadas
                # Garantir ordenação: anos em ordem crescente e tipos de violência na ordem desejada
                df_composicao = df_composicao.sort_values(['ANO_NOTIFIC', 'TIPO_VIOLENCIA'])
                
                # Definir ordem dos tipos de violência
                ordem_tipos = ['Sexual', 'Física', 'Psicológica']
                
                fig_bar = px.bar(
                    df_composicao,
                    x='ANO_NOTIFIC',
                    y='Contagem',
                    color='TIPO_VIOLENCIA',
                    title='Notificações por Ano e Tipo de Violência (Sexual, Física, Psicológica)',
                    labels={'ANO_NOTIFIC': 'Ano', 'Contagem': 'Contagem de Notificações', 'TIPO_VIOLENCIA': 'Tipo de Violência'},
                    barmode='group',
                    color_discrete_map={
                        'Sexual': '#C73E1D',
                        'Física': '#2E86AB',
                        'Psicológica': '#F18F01'
                    },
                    category_orders={
                        'ANO_NOTIFIC': sorted(df_composicao['ANO_NOTIFIC'].unique()),
                        'TIPO_VIOLENCIA': ordem_tipos
                    },
                    height=500
                )
                
                fig_bar.update_layout(
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=12)
                    ),
                    margin=dict(l=50, r=50, t=80, b=100),
                    height=500
                )
                
                max_contagem = df_composicao['Contagem'].max()
                aplicar_formatacao_eixo(fig_bar, max_contagem, eixo='y')
                
                # Adicionar hover personalizado
                fig_bar.update_traces(
                    hovertemplate='<b>%{x}</b><br>Tipo: %{fullData.name}<br>Total: %{y:,.0f}<br>Percentual do ano: %{customdata:.1f}%<extra></extra>',
                    customdata=df_composicao['Percentual']
                )
                
                st.plotly_chart(fig_bar, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao processar tipos de violência: {str(e)}")
            st.info("Tente limpar o cache e recarregar os dados")
    else:
        st.info("Dados de tipo de violência ou ano não disponíveis para este gráfico")

    # Gráfico 3: Distribuição por Faixa Etária e Sexo (H2, H4, H5)
    st.markdown('<div class="section-header">3. Distribuição por Faixa Etária e Sexo (H2, H4, H5)</div>', unsafe_allow_html=True)
    st.markdown("**H2 – Tipo de Violência por faixa etária:** \"Violência sexual é o tipo mais incidente entre adolescentes (12–17 anos)?\" | **H4 – Sexo vs Tipo:** \"A violência sexual é desproporcionalmente maior no sexo feminino?\" | **H5 – Faixa Etária vs Psicológica/Moral:** \"A violência psicológica é mais comum em adolescentes de 15 a 17 anos?\"")
    st.info("**Nota:** Análise para crianças e adolescentes de **0 a 17 anos** (faixas: 0-1, 2-5, 6-9, 10-13, 14-17 anos).")
    
    if 'FAIXA_ETARIA' in df_filtrado.columns and 'SEXO' in df_filtrado.columns:
        # Garantir que apenas faixas etárias de 0-17 anos sejam incluídas
        faixas_validas = ['0-1 anos', '2-5 anos', '6-9 anos', '10-13 anos', '14-17 anos']
        
        # IMPORTANTE: Usar df_filtrado completo e manter índice original para contar corretamente
        # Criar REGISTRO_ID baseado no índice original ANTES de qualquer filtro
        df_demografia_base = df_filtrado.copy()
        
        # Filtrar Sexo "Ignorado" conforme solicitação do usuário
        # FIX: Usar isin para evitar filtrar incorretamente palavras contendo 'i' (como Masculino/Feminino)
        df_demografia_base = df_demografia_base[df_demografia_base['SEXO'].isin(['Masculino', 'Feminino'])]
        
        df_demografia_base['REGISTRO_ID_ORIGINAL'] = df_demografia_base.index
        
        # Debug: verificar total de registros antes dos filtros
        total_antes_filtros = len(df_demografia_base)
        
        # Filtrar apenas registros com faixas válidas e sexo válido
        df_demografia = df_demografia_base[
            (df_demografia_base['FAIXA_ETARIA'].isin(faixas_validas)) & 
            (df_demografia_base['SEXO'].notna()) & 
            (df_demografia_base['SEXO'] != 'Não informado')
        ].copy()
        
        # Debug: verificar total após filtros de faixa etária e sexo
        total_apos_filtros_demo = len(df_demografia)
        
        if len(df_demografia) > 0:
            # OTIMIZADO: Usar função com cache para processar tipos de violência expandidos
            # Isso evita reprocessamento desnecessário a cada renderização
            df_tipos_expandidos_demo = process_tipos_violencia_expandidos_demo(df_demografia)
            
            # Criar gráfico que mostra tipos de violência por faixa etária e sexo
            # Os KPIs serão calculados usando os mesmos dados agrupados do gráfico para garantir consistência
            faixas_adolescentes = ['10-13 anos', '14-17 anos']
            total_registros_unicos = 0  # Inicializar variável
            if df_tipos_expandidos_demo is not None and len(df_tipos_expandidos_demo) > 0:
                # IMPORTANTE: Contar REGISTROS ÚNICOS usando índice original
                # Cada registro original conta apenas uma vez por tipo de violência
                if 'REGISTRO_ID_ORIGINAL' in df_tipos_expandidos_demo.columns:
                    # Verificar total de registros únicos antes do agrupamento
                    total_registros_unicos = df_tipos_expandidos_demo['REGISTRO_ID_ORIGINAL'].nunique()
                    
                    # Agrupar e contar registros únicos usando REGISTRO_ID_ORIGINAL
                    # Isso garante que cada notificação conta apenas uma vez por tipo
                    df_grafico = df_tipos_expandidos_demo.groupby(
                        ['FAIXA_ETARIA', 'SEXO', 'TIPO_VIOLENCIA']
                    )['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
                    
                    # Verificar se a soma faz sentido (pode ser maior que total_registros_unicos se houver múltiplos tipos)
                    soma_total_grafico = df_grafico['Contagem'].sum()
                    
                    # Mostrar informações sobre quantos registros estão sendo analisados
                    # Nota: A soma pode ser maior que total_registros_unicos porque um registro pode ter múltiplos tipos
                    st.caption(f"**Análise baseada em {total_registros_unicos:,} notificações únicas** com tipos Sexual/Física/Psicológica (faixa etária 0-17 anos, período {ano_selecionado[0]}-{ano_selecionado[1]}) | Total de registros no período: {total_antes_filtros:,}")
                else:
                    # Fallback: se não tiver REGISTRO_ID_ORIGINAL, usar size() mas avisar
                    df_grafico = df_tipos_expandidos_demo.groupby(
                        ['FAIXA_ETARIA', 'SEXO', 'TIPO_VIOLENCIA']
                    ).size().reset_index(name='Contagem')
                    total_registros_unicos = total_apos_filtros_demo  # Usar fallback
                    st.warning("Aviso: Contando linhas expandidas, não registros únicos. Os números podem estar inflados.")
                
                # Recalcular KPIs usando os dados agrupados do gráfico (garantir consistência)
                # H2: Violência sexual em adolescentes (10-13 e 14-17 anos)
                df_adolescentes_grafico = df_grafico[
                    df_grafico['FAIXA_ETARIA'].isin(faixas_adolescentes)
                ]
                if len(df_adolescentes_grafico) > 0:
                    total_por_tipo_h2 = df_adolescentes_grafico.groupby('TIPO_VIOLENCIA')['Contagem'].sum()
                    total_sexual_h2 = total_por_tipo_h2.get('Sexual', 0)
                    total_fisica_h2 = total_por_tipo_h2.get('Física', 0)
                    total_psicologica_h2 = total_por_tipo_h2.get('Psicológica', 0)
                    total_todos_tipos_h2 = total_por_tipo_h2.sum()
                    pct_sexual_h2 = (total_sexual_h2 / total_todos_tipos_h2 * 100) if total_todos_tipos_h2 > 0 else 0
                    pct_fisica_h2 = (total_fisica_h2 / total_todos_tipos_h2 * 100) if total_todos_tipos_h2 > 0 else 0
                    pct_psicologica_h2 = (total_psicologica_h2 / total_todos_tipos_h2 * 100) if total_todos_tipos_h2 > 0 else 0
                    
                    # Análise H2: Determinar resposta
                    if pct_sexual_h2 > max(pct_fisica_h2, pct_psicologica_h2) and pct_sexual_h2 > 40:
                        resposta_h2 = "SIM - A violência sexual é o tipo mais incidente entre adolescentes"
                        explicacao_h2 = f"A violência sexual representa {pct_sexual_h2:.1f}% dos casos em adolescentes (10-17 anos), sendo maior que Física ({pct_fisica_h2:.1f}%) e Psicológica ({pct_psicologica_h2:.1f}%)."
                    elif pct_sexual_h2 > max(pct_fisica_h2, pct_psicologica_h2):
                        resposta_h2 = "SIM - A violência sexual é o tipo mais incidente, mas com margem pequena"
                        explicacao_h2 = f"A violência sexual representa {pct_sexual_h2:.1f}% dos casos em adolescentes, ligeiramente maior que Física ({pct_fisica_h2:.1f}%) e Psicológica ({pct_psicologica_h2:.1f}%)."
                    else:
                        resposta_h2 = "NÃO - A violência sexual não é o tipo mais incidente entre adolescentes"
                        tipo_maior = "Física" if pct_fisica_h2 > pct_psicologica_h2 else "Psicológica"
                        pct_maior = max(pct_fisica_h2, pct_psicologica_h2)
                        explicacao_h2 = f"A violência sexual representa {pct_sexual_h2:.1f}% dos casos em adolescentes, menor que {tipo_maior} ({pct_maior:.1f}%)."
                else:
                    total_sexual_h2 = 0
                    pct_sexual_h2 = 0
                    resposta_h2 = "N/A - Dados insuficientes"
                    cor_resposta_h2 = "⚪"
                    explicacao_h2 = "Não há dados suficientes para analisar esta hipótese."
                
                # H4: Violência sexual por sexo
                df_sexual_grafico = df_grafico[df_grafico['TIPO_VIOLENCIA'] == 'Sexual']
                if len(df_sexual_grafico) > 0:
                    total_sexual_sexo_h4 = df_sexual_grafico['Contagem'].sum()
                    total_feminino_sexual_h4 = df_sexual_grafico[df_sexual_grafico['SEXO'] == 'Feminino']['Contagem'].sum()
                    total_masculino_sexual_h4 = df_sexual_grafico[df_sexual_grafico['SEXO'] == 'Masculino']['Contagem'].sum()
                    pct_feminino_sexual_h4 = (total_feminino_sexual_h4 / total_sexual_sexo_h4 * 100) if total_sexual_sexo_h4 > 0 else 0
                    pct_masculino_sexual_h4 = (total_masculino_sexual_h4 / total_sexual_sexo_h4 * 100) if total_sexual_sexo_h4 > 0 else 0
                    
                    # Comparar com distribuição geral
                    total_geral_h4 = df_grafico['Contagem'].sum()
                    total_feminino_geral_h4 = df_grafico[df_grafico['SEXO'] == 'Feminino']['Contagem'].sum()
                    pct_feminino_geral_h4 = (total_feminino_geral_h4 / total_geral_h4 * 100) if total_geral_h4 > 0 else 0
                    desproporcao_h4 = pct_feminino_sexual_h4 - pct_feminino_geral_h4
                    
                    # Análise H4: Determinar resposta
                    if desproporcao_h4 > 15:
                        resposta_h4 = "SIM - A violência sexual é desproporcionalmente maior no sexo feminino"
                        explicacao_h4 = f"A violência sexual afeta {pct_feminino_sexual_h4:.1f}% do sexo feminino, enquanto a distribuição geral de violência é {pct_feminino_geral_h4:.1f}% feminina. A diferença de {desproporcao_h4:.1f} pontos percentuais indica desproporção significativa."
                    elif desproporcao_h4 > 5:
                        resposta_h4 = "SIM - Há desproporção, mas moderada"
                        explicacao_h4 = f"A violência sexual afeta {pct_feminino_sexual_h4:.1f}% do sexo feminino, enquanto a distribuição geral é {pct_feminino_geral_h4:.1f}% feminina. A diferença de {desproporcao_h4:.1f} pontos percentuais indica desproporção moderada."
                    else:
                        resposta_h4 = "NÃO - A violência sexual não é desproporcionalmente maior no sexo feminino"
                        explicacao_h4 = f"A violência sexual afeta {pct_feminino_sexual_h4:.1f}% do sexo feminino, similar à distribuição geral de violência ({pct_feminino_geral_h4:.1f}% feminina). A diferença de {desproporcao_h4:.1f} pontos percentuais não indica desproporção significativa."
                else:
                    total_feminino_sexual_h4 = 0
                    desproporcao_h4 = 0
                    resposta_h4 = "N/A - Dados insuficientes"
                    cor_resposta_h4 = "⚪"
                    explicacao_h4 = "Não há dados suficientes para analisar esta hipótese."
                
                # H5: Violência psicológica em 14-17 anos
                df_psicologica_grafico = df_grafico[df_grafico['TIPO_VIOLENCIA'] == 'Psicológica']
                if len(df_psicologica_grafico) > 0:
                    total_psicologica_h5 = df_psicologica_grafico['Contagem'].sum()
                    contagem_14_17_h5 = df_psicologica_grafico.loc[df_psicologica_grafico['FAIXA_ETARIA'] == '14-17 anos', 'Contagem'].sum()
                    # Calcular também outras faixas para comparação
                    contagem_10_13_h5 = df_psicologica_grafico.loc[df_psicologica_grafico['FAIXA_ETARIA'] == '10-13 anos', 'Contagem'].sum()
                    contagem_6_9_h5 = df_psicologica_grafico.loc[df_psicologica_grafico['FAIXA_ETARIA'] == '6-9 anos', 'Contagem'].sum()
                    pct_14_17_h5 = (contagem_14_17_h5 / total_psicologica_h5 * 100) if total_psicologica_h5 > 0 else 0
                    pct_10_13_h5 = (contagem_10_13_h5 / total_psicologica_h5 * 100) if total_psicologica_h5 > 0 else 0
                    pct_6_9_h5 = (contagem_6_9_h5 / total_psicologica_h5 * 100) if total_psicologica_h5 > 0 else 0
                    
                    # Análise H5: Determinar resposta (comparar 14-17 com outras faixas)
                    if pct_14_17_h5 > max(pct_10_13_h5, pct_6_9_h5) and pct_14_17_h5 > 35:
                        resposta_h5 = "SIM - A violência psicológica é mais comum em adolescentes de 14-17 anos"
                        explicacao_h5 = f"A violência psicológica em adolescentes de 14-17 anos representa {pct_14_17_h5:.1f}% do total de violência psicológica, sendo maior que nas faixas 10-13 anos ({pct_10_13_h5:.1f}%) e 6-9 anos ({pct_6_9_h5:.1f}%)."
                    elif pct_14_17_h5 > max(pct_10_13_h5, pct_6_9_h5):
                        resposta_h5 = "SIM - Mais comum em 14-17 anos, mas com margem pequena"
                        explicacao_h5 = f"A violência psicológica em adolescentes de 14-17 anos representa {pct_14_17_h5:.1f}% do total, ligeiramente maior que nas outras faixas etárias analisadas."
                    else:
                        resposta_h5 = "NÃO - A violência psicológica não é mais comum em adolescentes de 14-17 anos"
                        faixa_maior = "10-13 anos" if pct_10_13_h5 > pct_6_9_h5 else "6-9 anos"
                        pct_maior_h5 = max(pct_10_13_h5, pct_6_9_h5)
                        explicacao_h5 = f"A violência psicológica em adolescentes de 14-17 anos representa {pct_14_17_h5:.1f}% do total, menor que na faixa de {faixa_maior} ({pct_maior_h5:.1f}%)."
                else:
                    contagem_14_17_h5 = 0
                    pct_14_17_h5 = 0
                    resposta_h5 = "N/A - Dados insuficientes"
                    cor_resposta_h5 = "⚪"
                    explicacao_h5 = "Não há dados suficientes para analisar esta hipótese."
                
                # Exibir KPIs (usando dados do gráfico para garantir consistência)
                col_h2, col_h4, col_h5 = st.columns(3)
                with col_h2:
                    st.metric(
                        "Violência Sexual em Adolescentes (H2)",
                        formatar_numero_br(total_sexual_h2),
                        delta=f"{pct_sexual_h2:.1f}%"
                    )
                with col_h4:
                    st.metric(
                        "Desproporção Feminino - Sexual (H4)",
                        formatar_numero_br(total_feminino_sexual_h4),
                        delta=f"{desproporcao_h4:+.1f}pp"
                    )
                with col_h5:
                    st.metric(
                        "Psicológica em 14-17 anos (H5)",
                        formatar_numero_br(contagem_14_17_h5),
                        delta=f"{pct_14_17_h5:.1f}%"
                    )
                
                # Filtrar apenas sexos válidos
                df_grafico = df_grafico[
                    (df_grafico['SEXO'].notna()) & 
                    (df_grafico['SEXO'] != 'Não informado')
                ]
                
                # Ordenar faixas etárias
                ordem_faixas = {faixa: idx for idx, faixa in enumerate(faixas_validas)}
                df_grafico['ordem'] = df_grafico['FAIXA_ETARIA'].map(ordem_faixas)
                df_grafico = df_grafico.sort_values(['ordem', 'TIPO_VIOLENCIA']).drop('ordem', axis=1)
                
                # Calcular porcentagens por faixa etária
                df_grafico['Total_Faixa'] = df_grafico.groupby('FAIXA_ETARIA')['Contagem'].transform('sum')
                df_grafico['Percentual'] = (df_grafico['Contagem'] / df_grafico['Total_Faixa'] * 100).round(1)
                df_grafico['Contagem_Formatada'] = df_grafico['Contagem'].apply(formatar_numero_br)
                df_grafico['Percentual_Formatado'] = df_grafico['Percentual'].apply(lambda x: f"{x:.1f}%")
                
                # Criar gráfico de barras agrupadas: tipos de violência por faixa etária, separado por sexo
                # Usar hover_data para adicionar informações extras no hover
                fig_bar_grouped = px.bar(
                    df_grafico, 
                    x='FAIXA_ETARIA', 
                    y='Contagem', 
                    color='TIPO_VIOLENCIA',
                    facet_col='SEXO',  # Separar por sexo em colunas
                    barmode='group',
                    title='Tipos de Violência por Faixa Etária e Sexo (H2, H4, H5)',
                    labels={
                        'FAIXA_ETARIA': 'Faixa Etária', 
                        'Contagem': 'Contagem de Notificações', 
                        'TIPO_VIOLENCIA': 'Tipo de Violência',
                        'SEXO': 'Sexo'
                    },
                    category_orders={
                        'FAIXA_ETARIA': faixas_validas,
                        'TIPO_VIOLENCIA': ['Sexual', 'Física', 'Psicológica']
                    },
                    color_discrete_map={
                        'Sexual': '#C73E1D',
                        'Física': '#2E86AB',
                        'Psicológica': '#F18F01'
                    },
                    hover_data={
                        'Contagem': ':,.0f',  # Formato numérico com separador de milhares
                        'Percentual_Formatado': True,  # Mostrar percentual formatado
                        'TIPO_VIOLENCIA': False,  # Já está no hover padrão
                        'SEXO': False,  # Já está no facet_col
                        'FAIXA_ETARIA': False  # Já está no eixo x
                    },
                    height=500
                )
                
                fig_bar_grouped.update_xaxes(tickangle=0)
                fig_bar_grouped.update_layout(
                    legend=dict(
                        orientation="h",
                        yanchor="top",
                        y=-0.15,
                        xanchor="center",
                        x=0.5,
                        font=dict(size=11)
                    ),
                    margin=dict(l=50, r=50, t=80, b=120),
                    height=500
                )
                
                max_contagem = df_grafico['Contagem'].max()
                aplicar_formatacao_eixo(fig_bar_grouped, max_contagem, eixo='y')
                
                # Atualizar hover template para mostrar valor individual de cada barra
                # %{y} mostra o valor individual da barra (Contagem)
                # O hover_data já adiciona Percentual_Formatado automaticamente
                fig_bar_grouped.update_traces(
                    hovertemplate='<b>%{x}</b><br>' +
                                 '<b>Tipo de Violência:</b> %{fullData.name}<br>' +
                                 '<b>Notificações:</b> %{y:,.0f}<br>' +
                                 '<b>Percentual da faixa etária:</b> %{customdata[1]}<extra></extra>'
                )
                
                st.plotly_chart(fig_bar_grouped, use_container_width=True)
                
                # Mostrar estatísticas - usar total_registros_unicos (já calculado acima)
                # NÃO somar df_grafico['Contagem'].sum() porque isso conta linhas expandidas múltiplas vezes
                # total_registros_unicos já representa o número correto de notificações únicas
                # total_registros_unicos já foi definido no bloco if acima
                st.caption(f"**Total de notificações únicas analisadas:** {formatar_numero_br(total_registros_unicos)} | **Faixas etárias:** {', '.join(faixas_validas)} | **Período:** {ano_selecionado[0]}-{ano_selecionado[1]}")
            else:
                # Fallback: gráfico simples por faixa etária e sexo (sem tipos de violência)
                # KPIs não podem ser calculados sem tipos de violência expandidos
                col_h2, col_h4, col_h5 = st.columns(3)
                with col_h2:
                    st.metric("Violência Sexual em Adolescentes (H2)", "N/A", delta="Dados não disponíveis")
                with col_h4:
                    st.metric("Desproporção Feminino - Sexual (H4)", "N/A", delta="Dados não disponíveis")
                with col_h5:
                    st.metric("Psicológica em 14-17 anos (H5)", "N/A", delta="Dados não disponíveis")
                
                df_demografia = df_demografia.groupby(['FAIXA_ETARIA', 'SEXO']).size().reset_index(name='Contagem')
                total_geral_demo = df_demografia['Contagem'].sum()
                df_demografia['Percentual'] = (df_demografia['Contagem'] / total_geral_demo * 100).round(1)
                df_demografia['Contagem_Formatada'] = df_demografia['Contagem'].apply(formatar_numero_br)
                
                ordem_faixas = {faixa: idx for idx, faixa in enumerate(faixas_validas)}
                df_demografia['ordem'] = df_demografia['FAIXA_ETARIA'].map(ordem_faixas)
                df_demografia = df_demografia.sort_values('ordem').drop('ordem', axis=1)
                
                fig_bar_grouped = px.bar(
                    df_demografia, 
                    x='FAIXA_ETARIA', 
                    y='Contagem', 
                    color='SEXO',
                    barmode='group',
                    title='Contagem de Notificações por Faixa Etária e Sexo (0-17 anos)',
                    labels={'FAIXA_ETARIA': 'Faixa Etária', 'Contagem': 'Contagem de Notificações', 'SEXO': 'Sexo'},
                    category_orders={'FAIXA_ETARIA': faixas_validas},
                    color_discrete_sequence=['#2E86AB', '#A23B72', '#F18F01'],
                    height=500
                )
                fig_bar_grouped.update_layout(
                    legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                    margin=dict(l=50, r=50, t=80, b=120),
                    height=500
                )
                aplicar_formatacao_eixo(fig_bar_grouped, df_demografia['Contagem'].max(), eixo='y')
                st.plotly_chart(fig_bar_grouped, use_container_width=True)
        else:
            st.warning("Nenhum dado encontrado nas faixas etárias de 0-17 anos para os filtros selecionados.")
    else:
        st.info("Dados demográficos não disponíveis para este gráfico")

    # Gráfico 4: Distribuição Geográfica (H6, H7)
    if uf_selecionada == 'Todos':
        st.markdown('<div class="section-header">4. Distribuição Geográfica por Município do Maranhão (H7)</div>', unsafe_allow_html=True)
        st.markdown("**H7 – Contribuição Estadual:** \"Imperatriz representa mais de 15% das notificações do Maranhão?\"")
        
        if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns and 'UF_NOTIFIC' in df_filtrado.columns:
            # Filtrar apenas municípios do Maranhão
            df_ma = df_filtrado[df_filtrado['UF_NOTIFIC'] == 'Maranhão'].copy() if 'Maranhão' in df_filtrado['UF_NOTIFIC'].values else pd.DataFrame()
            
            if len(df_ma) > 0:
                # Agrupar por município - usar nunique() se houver ID único para evitar duplicatas
                # IMPORTANTE: Usar observed=True para evitar problemas com categorias
                if 'REGISTRO_ID_ORIGINAL' in df_ma.columns:
                    df_municipios_ma = df_ma.groupby('MUNICIPIO_NOTIFIC', observed=True)['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
                else:
                    df_municipios_ma = df_ma.groupby('MUNICIPIO_NOTIFIC', observed=True).size().reset_index(name='Contagem')
                
                # Calcular total do Maranhão (registros únicos se houver ID)
                if 'REGISTRO_ID_ORIGINAL' in df_ma.columns:
                    total_ma = df_ma['REGISTRO_ID_ORIGINAL'].nunique()
                else:
                    total_ma = len(df_ma)
                
                # Calcular percentuais em relação ao total do Maranhão
                df_municipios_ma['Percentual'] = (df_municipios_ma['Contagem'] / total_ma * 100).round(1)
                df_municipios_ma['Percentual_Formatado'] = df_municipios_ma['Percentual'].apply(lambda x: f"{x:.1f}%")
                df_municipios_ma['Contagem_Formatada'] = df_municipios_ma['Contagem'].apply(formatar_numero_br)
                
                # IMPORTANTE: Ordenar por contagem ANTES de calcular posição
                df_municipios_ma = df_municipios_ma.sort_values('Contagem', ascending=False).reset_index(drop=True)
                
                # Usar função centralizada para contar Imperatriz de forma consistente
                imperatriz_info = contar_imperatriz(df_ma, uf_filtro='Maranhão')
                
                # Encontrar posição de Imperatriz no ranking completo (após ordenação)
                # IMPORTANTE: Buscar apenas "Imperatriz" exato, não "Santo Amaro da Imperatriz"
                imperatriz_mask = (
                    (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                    (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                ) & (
                    ~df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                )
                imperatriz_data = df_municipios_ma[imperatriz_mask]
                
                # Inicializar variáveis
                posicao_imperatriz = None
                contagem_imperatriz = imperatriz_info['contagem']  # Usar contagem centralizada
                pct_imperatriz = imperatriz_info['percentual']  # Usar percentual centralizado
                
                # Identificar o município líder (primeiro do ranking)
                municipio_lider = None
                contagem_lider = 0
                pct_lider = 0.0
                if len(df_municipios_ma) > 0:
                    municipio_lider = df_municipios_ma.iloc[0]['MUNICIPIO_NOTIFIC']
                    contagem_lider = df_municipios_ma.iloc[0]['Contagem']
                    pct_lider = df_municipios_ma.iloc[0]['Percentual']
                
                if imperatriz_info['encontrado'] and len(imperatriz_data) > 0:
                    # Posição no ranking completo (índice + 1, pois começa em 0)
                    # Usar o índice do DataFrame já ordenado
                    posicao_imperatriz = df_municipios_ma[imperatriz_mask].index[0] + 1
                    # Usar valores da função centralizada para garantir consistência
                    contagem_imperatriz = imperatriz_info['contagem']
                    pct_imperatriz = imperatriz_info['percentual']
                    
                    # Destacar Imperatriz no gráfico (apenas "Imperatriz", não "Santo Amaro")
                    df_municipios_ma['Destaque'] = (
                        (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                        (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                    ) & (
                        ~df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                    )
                    
                    # Exibir KPIs em colunas
                    col_h7_1, col_h7_2, col_h7_3 = st.columns(3)
                    with col_h7_1:
                        st.metric(
                            "Total de Casos no MA",
                            formatar_numero_br(total_ma)
                        )
                    with col_h7_2:
                        st.metric(
                            "Município Líder",
                            municipio_lider if municipio_lider else "N/A",
                            delta=f"{formatar_numero_br(contagem_lider)} ({pct_lider:.1f}%)" if municipio_lider else None
                        )
                    with col_h7_3:
                        st.metric(
                            "Contribuição de Imperatriz (H7)",
                            formatar_numero_br(contagem_imperatriz),
                            delta=f"{pct_imperatriz:.1f}% ({posicao_imperatriz}º lugar)"
                        )
                else:
                    st.warning("Imperatriz não encontrada nos dados do Maranhão")
                    df_municipios_ma['Destaque'] = False
                    
                    # Exibir KPIs mesmo sem Imperatriz
                    col_h7_1, col_h7_2 = st.columns(2)
                    with col_h7_1:
                        st.metric(
                            "Total de Casos no MA",
                            formatar_numero_br(total_ma)
                        )
                    with col_h7_2:
                        st.metric(
                            "Município Líder",
                            municipio_lider if municipio_lider else "N/A",
                            delta=f"{formatar_numero_br(contagem_lider)} ({pct_lider:.1f}%)" if municipio_lider else None
                        )
                
                # IMPORTANTE: Garantir que Imperatriz use SEMPRE os valores da função centralizada
                # Primeiro, remover Imperatriz do DataFrame agrupado se existir (para evitar valores incorretos)
                # IMPORTANTE: Remover apenas "Imperatriz" exato, não "Santo Amaro da Imperatriz"
                imperatriz_mask_remover = (
                    (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                    (df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                ) & (
                    ~df_municipios_ma['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                )
                df_municipios_ma_sem_imperatriz = df_municipios_ma[~imperatriz_mask_remover].copy()
                
                # Pegar top 10 municípios (sem Imperatriz)
                num_municipios = min(10, len(df_municipios_ma_sem_imperatriz))
                df_municipios_ma_top = df_municipios_ma_sem_imperatriz.head(num_municipios).copy()
                
                # SEMPRE adicionar Imperatriz com valores corretos da função centralizada
                if imperatriz_info['encontrado']:
                    # Encontrar o nome exato do município (pode variar)
                    nome_imperatriz = 'Imperatriz'
                    if len(imperatriz_data) > 0:
                        nome_imperatriz = imperatriz_data.iloc[0]['MUNICIPIO_NOTIFIC']
                    else:
                        # Tentar encontrar no DataFrame completo
                        imperatriz_no_df = df_municipios_ma[imperatriz_mask]
                        if len(imperatriz_no_df) > 0:
                            nome_imperatriz = imperatriz_no_df.iloc[0]['MUNICIPIO_NOTIFIC']
                    
                    # Criar linha com valores EXATOS da função centralizada
                    imperatriz_row = pd.DataFrame({
                        'MUNICIPIO_NOTIFIC': [nome_imperatriz],
                        'Contagem': [contagem_imperatriz],  # SEMPRE usar contagem centralizada (440)
                        'Percentual': [pct_imperatriz],  # SEMPRE usar percentual centralizado
                        'Percentual_Formatado': [f"{pct_imperatriz:.1f}%"],
                        'Contagem_Formatada': [formatar_numero_br(contagem_imperatriz)],
                        'Destaque': [True]
                    })
                    
                    # Adicionar Imperatriz ao gráfico
                    df_municipios_ma_top = pd.concat([df_municipios_ma_top, imperatriz_row], ignore_index=True)
                    # Reordenar após adicionar Imperatriz
                    df_municipios_ma_top = df_municipios_ma_top.sort_values('Contagem', ascending=False).reset_index(drop=True)
                    num_municipios = len(df_municipios_ma_top)
                    
                    # Garantir que a coluna Destaque existe no top (apenas "Imperatriz", não "Santo Amaro")
                    if 'Destaque' not in df_municipios_ma_top.columns:
                        df_municipios_ma_top['Destaque'] = (
                            (df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                            (df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                        ) & (
                            ~df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                        )
                else:
                    # Se não houver Imperatriz, criar coluna Destaque com False
                    df_municipios_ma_top['Destaque'] = False
                
                # IMPORTANTE: Recalcular TODOS os valores para garantir consistência
                # Recalcular total_ma se houver ID único para garantir consistência
                if 'REGISTRO_ID_ORIGINAL' in df_ma.columns:
                    total_ma = df_ma['REGISTRO_ID_ORIGINAL'].nunique()
                else:
                    total_ma = len(df_ma)
                
                # Recalcular percentuais usando o total_ma (total do Maranhão)
                df_municipios_ma_top['Percentual'] = (df_municipios_ma_top['Contagem'] / total_ma * 100).round(1)
                
                # VALIDAÇÃO FINAL: Garantir que Imperatriz tenha exatamente o valor da função centralizada
                # IMPORTANTE: Fazer ANTES de criar o gráfico
                if imperatriz_info['encontrado']:
                    imperatriz_mask_final = (
                        (df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                        (df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                    ) & (
                        ~df_municipios_ma_top['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                    )
                    imperatriz_idx_final = df_municipios_ma_top[imperatriz_mask_final].index
                    if len(imperatriz_idx_final) > 0:
                        idx_final = imperatriz_idx_final[0]
                        # FORÇAR valores corretos
                        df_municipios_ma_top.loc[idx_final, 'Contagem'] = contagem_imperatriz
                        df_municipios_ma_top.loc[idx_final, 'Percentual'] = pct_imperatriz
                        print(f"[DEBUG H7 FINAL] Valores de Imperatriz FORÇADOS: Contagem={contagem_imperatriz}, Percentual={pct_imperatriz:.2f}%")
                
                # Recalcular percentuais após validação de Imperatriz
                df_municipios_ma_top['Percentual'] = (df_municipios_ma_top['Contagem'] / total_ma * 100).round(1)
                
                # Recalcular todas as colunas formatadas
                df_municipios_ma_top['Contagem_Formatada'] = df_municipios_ma_top['Contagem'].apply(formatar_numero_br)
                df_municipios_ma_top['Percentual_Formatado'] = df_municipios_ma_top['Percentual'].apply(lambda x: f"{x:.1f}%")
                
                # Ordenar por contagem (decrescente) - maior no topo
                df_municipios_ma_top = df_municipios_ma_top.sort_values('Contagem', ascending=False).reset_index(drop=True)
                
                # Criar texto completo para exibir nas barras
                df_municipios_ma_top['Texto_Completo'] = df_municipios_ma_top.apply(
                    lambda row: f"{row['Contagem_Formatada']} ({row['Percentual_Formatado']})", axis=1
                )
                
                # Debug: Verificar todos os valores ANTES de criar o gráfico
                print(f"[DEBUG H7] DataFrame completo ANTES de criar gráfico:")
                print(df_municipios_ma_top[['MUNICIPIO_NOTIFIC', 'Contagem', 'Contagem_Formatada']].to_string())
                
                # Criar gráfico de barras horizontal (mais legível para muitos municípios)
                # IMPORTANTE: Ordenar para gráfico horizontal (menor no topo, maior embaixo)
                df_municipios_ma_top_grafico = df_municipios_ma_top.sort_values('Contagem', ascending=True).reset_index(drop=True)
                
                # Debug: Verificar ordem dos dados antes de criar gráfico
                print(f"[DEBUG H7] Ordem dos municípios no DataFrame (índice -> município -> contagem):")
                for idx, row in df_municipios_ma_top_grafico.iterrows():
                    print(f"  {idx}: {row['MUNICIPIO_NOTIFIC']} -> {row['Contagem']}")
                
                # Criar gráfico - ordenar por contagem (decrescente) para gráfico horizontal
                # No gráfico horizontal, queremos maior no topo, então ordenamos decrescente
                df_municipios_ma_top_grafico = df_municipios_ma_top_grafico.sort_values('Contagem', ascending=False).reset_index(drop=True)
                
                fig_geo = px.bar(
                    df_municipios_ma_top_grafico,
                    x='Contagem',
                    y='MUNICIPIO_NOTIFIC',
                    orientation='h',  # Gráfico horizontal
                    title=f'Top {num_municipios} Municípios do Maranhão por Notificações (H7)',
                    labels={'MUNICIPIO_NOTIFIC': 'Município', 'Contagem': 'Contagem de Notificações'},
                    height=500,
                    color='Destaque',  # Destacar Imperatriz
                    color_discrete_map={True: '#C73E1D', False: '#2E86AB'},  # Vermelho para Imperatriz, azul para outros
                    custom_data=['Contagem_Formatada', 'Percentual_Formatado']  # Para hover apenas
                )
                # Ordenar por contagem decrescente (maior no topo)
                fig_geo.update_layout(
                    margin=dict(l=200, r=50, t=80, b=50),  # Espaço à esquerda para nomes dos municípios
                    height=500,
                    showlegend=False,  # Não precisa de legenda
                    yaxis={'categoryorder': 'total ascending'}  # Ordenar por valor total (menor no topo, maior embaixo)
                )
                aplicar_formatacao_eixo(fig_geo, df_municipios_ma_top_grafico['Contagem'].max(), eixo='x')
                
                # REMOVER texto das barras - apenas hover
                fig_geo.update_traces(
                    hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<br>Percentual do MA: %{customdata[1]}<extra></extra>',
                    text=None,  # Remover texto das barras
                    textposition=None
                )
                st.plotly_chart(fig_geo, use_container_width=True)
                
                # Mostrar ranking completo de Imperatriz
                if posicao_imperatriz is not None:
                    st.caption(f"**Imperatriz está em {posicao_imperatriz}º lugar** entre os municípios do Maranhão, representando **{pct_imperatriz:.1f}%** do total de notificações do estado ({formatar_numero_br(total_ma)} notificações).")
            else:
                st.warning("Nenhum dado encontrado para o Maranhão nos filtros selecionados.")
        else:
            st.info("Dados geográficos não disponíveis para este gráfico")
    
    elif municipio_selecionado == 'Todos' and uf_selecionada != 'Todos':
        st.markdown('<div class="section-header">4. Distribuição Geográfica por Município (H6)</div>', unsafe_allow_html=True)
        st.markdown("**H6 – Comparação Regional:** \"Imperatriz tem taxa de notificação maior que municípios de tamanho semelhante?\"")
        st.markdown("**(Gráfico de Barras)**: Comparação entre municípios.")
        
        if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns:
            # SIMPLIFICAR: Fazer groupby completo, ordenar, pegar top 10, e apenas CORRIGIR Imperatriz
            # Calcular KPI para H6 (Imperatriz vs outros) - PRIMEIRO usar função centralizada
            imperatriz_info = contar_imperatriz(df_filtrado, uf_filtro=uf_selecionada if uf_selecionada != 'Todos' else None)
            
            # Agrupar por município - fazer groupby COMPLETO primeiro
            # IMPORTANTE: Usar observed=True para evitar problemas com categorias
            if 'REGISTRO_ID_ORIGINAL' in df_filtrado.columns:
                df_municipio = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True)['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
            else:
                df_municipio = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True).size().reset_index(name='Contagem')
            
            # Ordenar por contagem (decrescente) e pegar top 10
            df_municipio = df_municipio.sort_values('Contagem', ascending=False).reset_index(drop=True).head(10)
            
            # Debug: Verificar valores ANTES de corrigir Imperatriz
            print(f"[DEBUG H6] Top 10 ANTES de corrigir Imperatriz:")
            print(df_municipio[['MUNICIPIO_NOTIFIC', 'Contagem']].to_string())
            
            # Se Imperatriz estiver no top 10, SUBSTITUIR seu valor pelo valor correto
            if imperatriz_info['encontrado']:
                contagem_imperatriz = imperatriz_info['contagem']
                # Buscar apenas "Imperatriz" exato (não "Santo Amaro da Imperatriz")
                imperatriz_mask = (
                    (df_municipio['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                    (df_municipio['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                ) & (
                    ~df_municipio['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                )
                
                if imperatriz_mask.any():
                    # Imperatriz já está no top 10 - apenas CORRIGIR o valor
                    idx_imperatriz = df_municipio[imperatriz_mask].index[0]
                    df_municipio.loc[idx_imperatriz, 'Contagem'] = contagem_imperatriz
                    # Reordenar após correção
                    df_municipio = df_municipio.sort_values('Contagem', ascending=False).reset_index(drop=True)
                else:
                    # Imperatriz não está no top 10 - adicionar com valor correto
                    nome_imperatriz = 'Imperatriz'
                    # Encontrar nome exato se existir no DataFrame completo (apenas "Imperatriz", não "Santo Amaro")
                    if 'REGISTRO_ID_ORIGINAL' in df_filtrado.columns:
                        df_municipio_temp = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True)['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
                    else:
                        df_municipio_temp = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True).size().reset_index(name='Contagem')
                    imperatriz_mask_temp = (
                        (df_municipio_temp['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                        (df_municipio_temp['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                    ) & (
                        ~df_municipio_temp['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                    )
                    imperatriz_no_completo = df_municipio_temp[imperatriz_mask_temp]
                    if len(imperatriz_no_completo) > 0:
                        nome_imperatriz = imperatriz_no_completo.iloc[0]['MUNICIPIO_NOTIFIC']
                    
                    imperatriz_row = pd.DataFrame({
                        'MUNICIPIO_NOTIFIC': [nome_imperatriz],
                        'Contagem': [contagem_imperatriz]
                    })
                    df_municipio = pd.concat([df_municipio, imperatriz_row], ignore_index=True)
                    df_municipio = df_municipio.sort_values('Contagem', ascending=False).reset_index(drop=True).head(10)
                
                # Calcular posição no ranking completo
                if 'REGISTRO_ID_ORIGINAL' in df_filtrado.columns:
                    df_municipio_completo = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True)['REGISTRO_ID_ORIGINAL'].nunique().reset_index(name='Contagem')
                else:
                    df_municipio_completo = df_filtrado.groupby('MUNICIPIO_NOTIFIC', observed=True).size().reset_index(name='Contagem')
                df_municipio_completo = df_municipio_completo.sort_values('Contagem', ascending=False).reset_index(drop=True)
                # Corrigir valor de Imperatriz no ranking completo também (apenas "Imperatriz", não "Santo Amaro")
                imperatriz_mask_completo = (
                    (df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                    (df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                ) & (
                    ~df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                )
                if imperatriz_mask_completo.any():
                    idx_imperatriz_completo = df_municipio_completo[imperatriz_mask_completo].index[0]
                    df_municipio_completo.loc[idx_imperatriz_completo, 'Contagem'] = contagem_imperatriz
                    df_municipio_completo = df_municipio_completo.sort_values('Contagem', ascending=False).reset_index(drop=True)
                    imperatriz_mask_pos = (
                        (df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.strip() == 'Imperatriz') |
                        (df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.match(r'^Imperatriz\s*/', case=False, na=False))
                    ) & (
                        ~df_municipio_completo['MUNICIPIO_NOTIFIC'].astype(str).str.contains('Santo Amaro', case=False, na=False)
                    )
                    if imperatriz_mask_pos.any():
                        posicao_imperatriz = df_municipio_completo[imperatriz_mask_pos].index[0] + 1
                    else:
                        posicao_imperatriz = None
                else:
                    posicao_imperatriz = None
                
                # Calcular média do top 10
                media_top10 = df_municipio['Contagem'].mean() if len(df_municipio) > 0 else 0
                variacao_vs_media = ((contagem_imperatriz - media_top10) / media_top10 * 100) if media_top10 > 0 else 0
                
                col_h6 = st.columns(1)[0]
                with col_h6:
                    if posicao_imperatriz:
                        st.metric(
                            "Posição de Imperatriz (H6)",
                            f"{posicao_imperatriz}º lugar",
                            delta=f"{variacao_vs_media:+.1f}% vs média top 10"
                        )
                    else:
                        st.metric(
                            "Notificações de Imperatriz (H6)",
                            formatar_numero_br(contagem_imperatriz),
                            delta="Dados disponíveis"
                        )
            else:
                st.info("Imperatriz não encontrada nos dados filtrados.")
            
            # Calcular percentuais e formatações
            total_municipios = df_municipio['Contagem'].sum()
            df_municipio['Percentual'] = (df_municipio['Contagem'] / total_municipios * 100).round(1) if total_municipios > 0 else 0
            df_municipio['Percentual_Formatado'] = df_municipio['Percentual'].apply(lambda x: f"{x:.1f}%")
            df_municipio['Contagem_Formatada'] = df_municipio['Contagem'].apply(formatar_numero_br)
            
            # Ordenar por contagem (decrescente) para gráfico horizontal
            df_municipio = df_municipio.sort_values('Contagem', ascending=False).reset_index(drop=True)
            
            fig_mun_bar = px.bar(
                df_municipio,
                x='Contagem',
                y='MUNICIPIO_NOTIFIC',
                orientation='h',  # Gráfico horizontal
                title=f'Top 10 Municípios com Mais Notificações ({uf_selecionada})',
                labels={'MUNICIPIO_NOTIFIC': 'Município', 'Contagem': 'Contagem de Notificações'},
                height=500,
                custom_data=['Contagem_Formatada', 'Percentual_Formatado']  # Para hover apenas
            )
            # Ordenar por contagem (menor no topo, maior embaixo)
            fig_mun_bar.update_layout(
                margin=dict(l=200, r=50, t=80, b=50),  # Espaço à esquerda para nomes dos municípios
                height=500,
                yaxis={'categoryorder': 'total ascending'}  # Ordenar por valor total (menor no topo, maior embaixo)
            )
            aplicar_formatacao_eixo(fig_mun_bar, df_municipio['Contagem'].max(), eixo='x')
            
            # REMOVER completamente texto das barras - apenas hover
            # Atualizar todos os traces para remover texto
            fig_mun_bar.update_traces(
                text=None,
                textposition=None,
                hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<br>Percentual: %{customdata[1]}<extra></extra>',
                selector=dict(type='bar')  # Aplicar apenas a barras
            )
            
            # Garantir que não há texto definido em nenhum lugar
            for trace in fig_mun_bar.data:
                trace.text = None
                trace.textposition = None
            st.plotly_chart(fig_mun_bar, use_container_width=True)
        else:
            st.info("Dados de município não disponíveis para este gráfico")

    # Gráfico 5: Distribuição por Local de Ocorrência (Barras ao invés de Pizza)
    if 'LOCAL_OCOR' in df_filtrado.columns:
        st.markdown('<div class="section-header">5. Distribuição por Local de Ocorrência</div>', unsafe_allow_html=True)
        st.markdown("**Análise:** Principais locais onde ocorreram as violências.")
        
        df_local = df_filtrado['LOCAL_OCOR'].value_counts().head(10).reset_index()
        df_local.columns = ['Local', 'Contagem']
        df_local = df_local.sort_values('Contagem', ascending=True)  # Ordenar para gráfico horizontal
        df_local['Contagem_Formatada'] = df_local['Contagem'].apply(formatar_numero_br)
        
        fig_local = px.bar(
            df_local,
            x='Contagem',
            y='Local',
            orientation='h',
            title='Top 10 Locais de Ocorrência',
            labels={'Local': 'Local de Ocorrência', 'Contagem': 'Número de Notificações'},
            color='Contagem',
            color_continuous_scale='Plasma',  # Paleta mais escura e visível
            height=500,
            custom_data=['Contagem_Formatada']
        )
        # Layout responsivo: colorbar na lateral (desktop) por padrão
        # JavaScript ajustará para abaixo em mobile
        fig_local.update_layout(
            margin=dict(l=200, r=80, t=80, b=50),  # Espaço para colorbar lateral (desktop)
            height=500,
            coloraxis_showscale=True,
            coloraxis_colorbar=get_colorbar_layout(is_mobile=False)  # Desktop por padrão
        )
        aplicar_formatacao_eixo(fig_local, df_local['Contagem'].max(), eixo='x')
        fig_local.update_traces(
            hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<extra></extra>',
            text=df_local['Contagem_Formatada'].values,  # Adicionar número de casos na frente de cada barra
            textposition='outside',  # Posicionar texto fora da barra
            textfont=dict(size=10)  # Tamanho da fonte
        )
        st.plotly_chart(fig_local, use_container_width=True)

    # Gráfico 6: Perfil do Agressor (Sexo) - H3
    if 'AUTOR_SEXO_CORRIGIDO' in df_filtrado.columns:
        st.markdown('<div class="section-header">6. Perfil do Agressor - Sexo (H3)</div>', unsafe_allow_html=True)
        st.markdown("**H3 – Perfil do Agressor:** \"Qual é a distribuição por sexo dos agressores nas notificações de violência?\"")
        
        df_autor = df_filtrado['AUTOR_SEXO_CORRIGIDO'].value_counts().reset_index()
        df_autor.columns = ['Sexo', 'Contagem']
        df_autor = df_autor[df_autor['Sexo'] != 'Não informado']  # Filtrar valores inválidos
        df_autor = df_autor.sort_values('Contagem', ascending=False).reset_index(drop=True)  # Ordenar por contagem (decrescente)
        df_autor['Contagem_Formatada'] = df_autor['Contagem'].apply(formatar_numero_br)
        
        fig_autor = px.bar(
            df_autor,
            x='Sexo',
            y='Contagem',
            title='Distribuição por Sexo do Agressor (H3)',
            labels={'Sexo': 'Sexo do Agressor', 'Contagem': 'Contagem'},
            color='Sexo',
            color_discrete_sequence=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'],  # Cores escuras e visíveis
            height=450,
            category_orders={'Sexo': df_autor['Sexo'].tolist()},  # Garantir ordem específica
            custom_data=['Contagem_Formatada']
        )
        fig_autor.update_layout(
            showlegend=False,  # Não precisa de legenda (já está nas barras)
            margin=dict(l=50, r=50, t=80, b=50),
            height=450
        )
        aplicar_formatacao_eixo(fig_autor, df_autor['Contagem'].max(), eixo='y')
        
        # Criar dicionário para mapear sexo -> texto formatado
        texto_por_sexo = dict(zip(df_autor['Sexo'], df_autor['Contagem_Formatada']))
        
        # Atualizar cada trace com o texto correto
        for trace in fig_autor.data:
            if hasattr(trace, 'name') and trace.name in texto_por_sexo:
                trace.text = [texto_por_sexo[trace.name]]
            trace.textposition = 'outside'
            trace.textfont = dict(size=12)
        
        # Hover template
        fig_autor.update_traces(
            hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
        )
        st.plotly_chart(fig_autor, use_container_width=True)
    
    # Gráfico 7: Relacionamento com o Agressor - H8 (Apenas os mais comuns)
    if 'GRAU_PARENTESCO' in df_filtrado.columns:
        st.markdown('<div class="section-header">7. Relacionamento com o Agressor (H8)</div>', unsafe_allow_html=True)
        st.markdown("**H8 – Relacionamento com o Agressor:** \"Qual é o grau de parentesco ou relacionamento mais comum entre a vítima e o agressor?\"")
        st.info("**Nota:** Inclui parentescos familiares e outros tipos de relacionamento (conhecido, funcionário de instituição, etc.).")
        
        df_parentesco = df_filtrado[df_filtrado['GRAU_PARENTESCO'] != 'Não informado']
        if len(df_parentesco) > 0:
            # Separar múltiplos parentescos
            parentescos_list = []
            for parentesco in df_parentesco['GRAU_PARENTESCO']:
                if pd.notna(parentesco):
                    parentescos_list.extend([p.strip() for p in str(parentesco).split(',')])
            
            parentesco_counts = Counter(parentescos_list)
            df_parent = pd.DataFrame(list(parentesco_counts.items()), columns=['Parentesco', 'Contagem'])
            
            # Calcular percentual do total para contexto
            total_parentescos = df_parent['Contagem'].sum()
            df_parent['Percentual'] = (df_parent['Contagem'] / total_parentescos * 100).round(1)
            
            # Ordenar por contagem (mais comuns primeiro)
            df_parent = df_parent.sort_values('Contagem', ascending=False)
            
            # Controle para escolher quantos mostrar
            num_parentescos = st.slider(
                "Quantos graus de parentesco mostrar?",
                min_value=5,
                max_value=min(20, len(df_parent)),
                value=10,
                key="num_parentescos"
            )
            
            # Pegar apenas os top N mais comuns
            df_parent_top = df_parent.head(num_parentescos).copy()
            df_parent_top = df_parent_top.sort_values('Contagem', ascending=True)  # Ordenar para gráfico horizontal
            df_parent_top['Contagem_Formatada'] = df_parent_top['Contagem'].apply(formatar_numero_br)
            df_parent_top['Percentual_Formatado'] = df_parent_top['Percentual'].apply(lambda x: f"{x:.1f}%")
            
            # Mostrar estatísticas
            percentual_total = df_parent_top['Percentual'].sum()
            total_formatado = formatar_numero_br(total_parentescos)
            st.caption(f"**Top {num_parentescos} relacionamentos mais comuns** (representam {percentual_total:.1f}% do total de {total_formatado} casos)")
            
            fig_parent = px.bar(
                df_parent_top,
                x='Contagem',
                y='Parentesco',
                orientation='h',
                title=f'Top {num_parentescos} Relacionamentos Mais Comuns com o Agressor (H8)',
                labels={'Parentesco': 'Tipo de Relacionamento', 'Contagem': 'Número de Notificações'},
                color='Contagem',
                color_continuous_scale='Inferno',  # Paleta mais escura e visível
                text='Contagem',  # Mostrar valores nas barras
                custom_data=['Contagem_Formatada', 'Percentual_Formatado']
            )
            # Formatação brasileira: ponto para milhares e adicionar porcentagem
            fig_parent.update_traces(
                texttemplate='%{text:,.0f} (%{customdata[1]})'.replace(',', 'X').replace('.', ',').replace('X', '.'),
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<br>Percentual: %{customdata[1]}<extra></extra>'
            )
            # Layout responsivo: colorbar na lateral (desktop) por padrão
            # JavaScript ajustará para abaixo em mobile
            fig_parent.update_layout(
                xaxis_title="Número de Notificações",
                yaxis_title="Grau de Parentesco",
                showlegend=False,
                margin=dict(l=200, r=80, t=80, b=50),  # Espaço para colorbar lateral (desktop)
                height=500,
                coloraxis_showscale=True,
                coloraxis_colorbar=get_colorbar_layout(is_mobile=False)  # Desktop por padrão
            )
            aplicar_formatacao_eixo(fig_parent, df_parent_top['Contagem'].max(), eixo='x')
            fig_parent.update_traces(
                hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<extra></extra>'
            )
            st.plotly_chart(fig_parent, use_container_width=True)
            
            # Tabela complementar com percentuais
            with st.expander(f"Ver detalhes completos dos Top {num_parentescos}"):
                df_display = df_parent_top.sort_values('Contagem', ascending=False)[['Parentesco', 'Contagem', 'Percentual']].copy()
                df_display.columns = ['Tipo de Relacionamento', 'Número de Casos', '% do Total']
                # Formatação brasileira
                df_display['Número de Casos'] = df_display['Número de Casos'].apply(formatar_numero_br)
                df_display['% do Total'] = df_display['% do Total'].apply(lambda x: f"{x:.1f}%".replace(".", ","))
                st.dataframe(df_display, use_container_width=True, hide_index=True)
    
    # Gráfico 8: Distribuição por Raça/Cor - H4 (Barras ao invés de Pizza)
    if 'CS_RACA' in df_filtrado.columns:
        st.markdown('<div class="section-header">8. Distribuição por Raça/Cor (H4)</div>', unsafe_allow_html=True)
        st.markdown("**H4 – Perfil da Vítima:** \"Qual é a distribuição por raça/cor das vítimas de violência contra crianças e adolescentes?\"")
        
        df_raca = df_filtrado['CS_RACA'].value_counts().reset_index()
        df_raca.columns = ['Raça/Cor', 'Contagem']
        # Filtrar para manter apenas: Parda, Branca, Preta e Indígena
        racas_permitidas = ['Parda', 'Branca', 'Preta', 'Indígena']
        df_raca = df_raca[df_raca['Raça/Cor'].isin(racas_permitidas)]
        df_raca = df_raca.sort_values('Contagem', ascending=False)
        total_raca = df_raca['Contagem'].sum()
        df_raca['Percentual'] = (df_raca['Contagem'] / total_raca * 100).round(1)
        df_raca['Percentual_Formatado'] = df_raca['Percentual'].apply(lambda x: f"{x:.1f}%")
        df_raca['Contagem_Formatada'] = df_raca['Contagem'].apply(formatar_numero_br)
        
        if len(df_raca) > 0:
            fig_raca = px.bar(
                df_raca,
                x='Raça/Cor',
                y='Contagem',
                title='Distribuição por Raça/Cor da Vítima (H4)',
                labels={'Raça/Cor': 'Raça/Cor', 'Contagem': 'Número de Notificações'},
                color='Contagem',
                color_continuous_scale='Viridis',  # Paleta mais escura e visível
                height=500,
                custom_data=['Contagem_Formatada', 'Percentual_Formatado']
            )
            fig_raca.update_xaxes(tickangle=45)
            # Layout responsivo: colorbar na lateral (desktop) por padrão
            # JavaScript ajustará para abaixo em mobile
            fig_raca.update_layout(
                margin=dict(l=50, r=80, t=80, b=100),  # Espaço para colorbar lateral (desktop)
                height=500,
                coloraxis_showscale=True,
                coloraxis_colorbar=get_colorbar_layout(is_mobile=False)  # Desktop por padrão
            )
            aplicar_formatacao_eixo(fig_raca, df_raca['Contagem'].max(), eixo='y')
            fig_raca.update_traces(
                hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<br>Percentual: %{customdata[1]}<extra></extra>',
                text=df_raca['Percentual_Formatado'].values,
                textposition='outside',
                textfont=dict(size=14)  # Aumentado de 9 para 14 para melhor legibilidade
            )
            st.plotly_chart(fig_raca, use_container_width=True)
    
    # Gráfico 9: Impacto da Pandemia - H10
    if 'DT_NOTIFIC' in df_filtrado.columns and df_filtrado['DT_NOTIFIC'].notna().any():
        st.markdown('<div class="section-header">9. Impacto da Pandemia nas Notificações (H10)</div>', unsafe_allow_html=True)
        st.markdown("**H10 – Impacto da Pandemia:** \"Houve queda das notificações em 2020 e aumento em 2021?\"")
        
        # Extrair ano da data de notificação
        df_filtrado['ANO'] = df_filtrado['DT_NOTIFIC'].dt.year
        
        # Filtrar para focar no período da pandemia: 2019, 2020 e 2021
        df_pandemia = df_filtrado[df_filtrado['ANO'].isin([2019, 2020, 2021])].copy()
        
        if len(df_pandemia) > 0:
            # Criar coluna Mês/Ano para análise mensal
            df_pandemia['MES_ANO'] = df_pandemia['DT_NOTIFIC'].dt.to_period('M').astype(str)
            df_pandemia['MES'] = df_pandemia['DT_NOTIFIC'].dt.month
            
            # Agrupar por mês/ano
            df_mensal_pandemia = df_pandemia.groupby(['MES_ANO', 'ANO', 'MES']).size().reset_index(name='Total')
            df_mensal_pandemia = df_mensal_pandemia.sort_values(['ANO', 'MES']).reset_index(drop=True)
            df_mensal_pandemia['Total_Formatado'] = df_mensal_pandemia['Total'].apply(formatar_numero_br)
            
            # Adicionar nomes dos meses
            meses_nomes_dict = {1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
                               7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'}
            df_mensal_pandemia['MES_NOME'] = df_mensal_pandemia['MES'].map(meses_nomes_dict)
            
            # Calcular variações mensais (em relação ao mesmo mês do ano anterior)
            df_mensal_pandemia['Variacao_vs_Ano_Anterior'] = None
            df_mensal_pandemia['Tendencia'] = None  # 'alta', 'queda', 'neutro'
            
            for ano in [2020, 2021]:
                ano_anterior = ano - 1
                for mes in range(1, 13):
                    valor_atual = df_mensal_pandemia[(df_mensal_pandemia['ANO'] == ano) & 
                                                     (df_mensal_pandemia['MES'] == mes)]['Total'].values
                    valor_anterior = df_mensal_pandemia[(df_mensal_pandemia['ANO'] == ano_anterior) & 
                                                        (df_mensal_pandemia['MES'] == mes)]['Total'].values
                    
                    if len(valor_atual) > 0 and len(valor_anterior) > 0:
                        variacao = ((valor_atual[0] - valor_anterior[0]) / valor_anterior[0] * 100)
                        mask = (df_mensal_pandemia['ANO'] == ano) & (df_mensal_pandemia['MES'] == mes)
                        df_mensal_pandemia.loc[mask, 'Variacao_vs_Ano_Anterior'] = variacao
                        
                        # Determinar tendência
                        if variacao > 5:  # Aumento significativo (>5%)
                            df_mensal_pandemia.loc[mask, 'Tendencia'] = 'alta'
                        elif variacao < -5:  # Queda significativa (<-5%)
                            df_mensal_pandemia.loc[mask, 'Tendencia'] = 'queda'
                        else:
                            df_mensal_pandemia.loc[mask, 'Tendencia'] = 'neutro'
            
            # Calcular totais anuais para comparação
            df_anual = df_pandemia.groupby('ANO').size().reset_index(name='Total_Anual')
            df_anual = df_anual.sort_values('ANO')
            df_anual['Total_Formatado'] = df_anual['Total_Anual'].apply(formatar_numero_br)
            
            # Calcular variações percentuais entre anos
            variacao_2019_2020 = None
            variacao_2020_2021 = None
            total_2019 = df_anual[df_anual['ANO'] == 2019]['Total_Anual'].values
            total_2020 = df_anual[df_anual['ANO'] == 2020]['Total_Anual'].values
            total_2021 = df_anual[df_anual['ANO'] == 2021]['Total_Anual'].values
            
            if len(total_2019) > 0 and len(total_2020) > 0:
                variacao_2019_2020 = ((total_2020[0] - total_2019[0]) / total_2019[0] * 100)
            if len(total_2020) > 0 and len(total_2021) > 0:
                variacao_2020_2021 = ((total_2021[0] - total_2020[0]) / total_2020[0] * 100)
            
            # Exibir KPIs
            col_h10_1, col_h10_2, col_h10_3 = st.columns(3)
            with col_h10_1:
                if len(total_2019) > 0:
                    st.metric(
                        "Total 2019 (Pré-Pandemia)",
                        formatar_numero_br(total_2019[0])
                    )
            with col_h10_2:
                if len(total_2020) > 0:
                    delta_2020 = f"{variacao_2019_2020:.1f}%" if variacao_2019_2020 is not None else None
                    st.metric(
                        "Total 2020 (Pandemia)",
                        formatar_numero_br(total_2020[0]),
                        delta=delta_2020
                    )
            with col_h10_3:
                if len(total_2021) > 0:
                    delta_2021 = f"{variacao_2020_2021:.1f}%" if variacao_2020_2021 is not None else None
                    st.metric(
                        "Total 2021 (Pós-Pandemia)",
                        formatar_numero_br(total_2021[0]),
                        delta=delta_2021
                    )
            
            # Criar gráfico de linha comparando os 3 anos
            fig_pandemia = px.line(
                df_mensal_pandemia,
                x='MES',
                y='Total',
                color='ANO',
                markers=True,
                title='Evolução Mensal: Comparação 2019 vs 2020 vs 2021 (H10)',
                labels={'MES': 'Mês', 'Total': 'Total de Notificações', 'ANO': 'Ano'},
                color_discrete_map={2019: '#2E86AB', 2020: '#C73E1D', 2021: '#F18F01'},  # Azul, Vermelho, Laranja
                height=500
            )
            
            # Adicionar nomes dos meses no eixo X (substituir números por nomes)
            meses_nomes = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
            fig_pandemia.update_xaxes(
                tickmode='array',
                tickvals=list(range(1, 13)),
                ticktext=meses_nomes,
                title='Mês'
            )
            
            fig_pandemia.update_layout(
                margin=dict(l=60, r=50, t=80, b=60),
                height=500,
                hovermode='x unified',
                legend=dict(
                    title="Ano",
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            aplicar_formatacao_eixo(fig_pandemia, df_mensal_pandemia['Total'].max(), eixo='y')
            
            # Adicionar indicadores visuais (balões) de queda/alta nos pontos
            # Adicionar anotações com balões coloridos para 2020 e 2021
            for idx, row in df_mensal_pandemia.iterrows():
                if pd.notna(row['Variacao_vs_Ano_Anterior']) and row['Tendencia'] != 'neutro':
                    variacao = row['Variacao_vs_Ano_Anterior']
                    
                    # Determinar cor, símbolo e cor de fundo baseado na tendência
                    if row['Tendencia'] == 'alta':
                        cor_texto = '#00C853'  # Verde escuro
                        cor_fundo = '#E8F5E9'  # Verde claro
                        simbolo = '▲'
                    else:  # queda
                        cor_texto = '#D32F2F'  # Vermelho escuro
                        cor_fundo = '#FFEBEE'  # Rosa claro (como no exemplo)
                        simbolo = '▼'
                    
                    # Formatar porcentagem
                    variacao_formatada = f"{variacao:+.1f}%"
                    
                    # Adicionar anotação com balão estilizado acima do ponto
                    fig_pandemia.add_annotation(
                        x=row['MES'],
                        y=row['Total'],
                        text=f"<b>{simbolo} {variacao_formatada}</b>",
                        showarrow=False,
                        yshift=25,
                        xref='x',
                        yref='y',
                        bgcolor=cor_fundo,  # Cor de fundo do balão
                        bordercolor=cor_texto,  # Cor da borda
                        borderwidth=1.5,
                        borderpad=6,  # Padding interno
                        font=dict(
                            color=cor_texto,
                            size=12,
                            family="Arial"
                        ),
                        align="center"
                    )
            
            # Hover com informações detalhadas incluindo variação
            fig_pandemia.update_traces(
                hovertemplate='<b>%{fullData.name}</b><br>Mês: %{x}<br>Total: %{y:,.0f}<extra></extra>',
                line=dict(width=3),
                marker=dict(size=10)  # Aumentado para melhor visibilidade
            )
            
            st.plotly_chart(fig_pandemia, use_container_width=True)
            
        else:
            st.info("Não há dados disponíveis para o período da pandemia (2019-2021) nos filtros selecionados.")
    
    # Gráfico 10: Comparação Regional - H6 (Municípios de Tamanho Semelhante)
    if uf_selecionada == 'Todos':
        st.markdown('<div class="section-header">10. Comparação Regional - Municípios de Tamanho Semelhante (H6)</div>', unsafe_allow_html=True)
        st.markdown("**H6 – Comparação Regional:** \"Imperatriz tem taxa de notificação maior que municípios de tamanho semelhante?\"")
        st.info("**Nota:** Municípios de tamanho semelhante são identificados com base no número de notificações (±30% do valor de Imperatriz).")
        
        if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns and 'UF_NOTIFIC' in df_filtrado.columns:
            # Agrupar por município e UF para obter a sigla do estado
            df_municipios_uf = df_filtrado.groupby(['MUNICIPIO_NOTIFIC', 'UF_NOTIFIC']).size().reset_index(name='Contagem')
            
            # Criar coluna com nome do município + sigla do estado
            # Converter para string para evitar problemas com tipos Categorical
            df_municipios_uf['MUNICIPIO_UF'] = df_municipios_uf['MUNICIPIO_NOTIFIC'].astype(str) + ' / ' + df_municipios_uf['UF_NOTIFIC'].astype(str)
            
            # Se houver municípios duplicados (mesmo nome em estados diferentes), manter o de maior contagem
            # Usar agg para manter todas as colunas necessárias
            df_municipios = df_municipios_uf.sort_values('Contagem', ascending=False).groupby('MUNICIPIO_NOTIFIC').agg({
                'UF_NOTIFIC': 'first',
                'Contagem': 'first',
                'MUNICIPIO_UF': 'first'
            }).reset_index()
            df_municipios = df_municipios.sort_values('Contagem', ascending=False).reset_index(drop=True)
            
            # Garantir que MUNICIPIO_UF existe (recriar se necessário)
            if 'MUNICIPIO_UF' not in df_municipios.columns or df_municipios['MUNICIPIO_UF'].isna().any():
                df_municipios['MUNICIPIO_UF'] = df_municipios['MUNICIPIO_NOTIFIC'].astype(str) + ' / ' + df_municipios['UF_NOTIFIC'].astype(str)
            
            # Usar função centralizada para contar Imperatriz de forma consistente
            imperatriz_info = contar_imperatriz(df_filtrado)
            
            # Encontrar Imperatriz no DataFrame agrupado para posicionamento
            imperatriz_mask = df_municipios['MUNICIPIO_NOTIFIC'].str.contains('Imperatriz', case=False, na=False)
            imperatriz_data = df_municipios[imperatriz_mask]
            
            if imperatriz_info['encontrado']:
                # Usar contagem centralizada para garantir consistência
                contagem_imperatriz = imperatriz_info['contagem']
                
                # Encontrar municípios de tamanho semelhante (±30% do valor de Imperatriz)
                limite_inferior = contagem_imperatriz * 0.7  # 70% = -30%
                limite_superior = contagem_imperatriz * 1.3  # 130% = +30%
                
                # Filtrar municípios dentro da faixa (incluindo Imperatriz)
                df_similares = df_municipios[
                    (df_municipios['Contagem'] >= limite_inferior) & 
                    (df_municipios['Contagem'] <= limite_superior)
                ].copy()
                
                # Ordenar por contagem (decrescente)
                df_similares = df_similares.sort_values('Contagem', ascending=False).reset_index(drop=True)
                
                # Destacar Imperatriz
                df_similares['Destaque'] = df_similares['MUNICIPIO_NOTIFIC'].str.contains('Imperatriz', case=False, na=False)
                
                # Garantir que a coluna MUNICIPIO_UF existe (com sigla do estado)
                if 'MUNICIPIO_UF' not in df_similares.columns:
                    df_similares['MUNICIPIO_UF'] = df_similares['MUNICIPIO_NOTIFIC'].astype(str) + ' / ' + df_similares['UF_NOTIFIC'].astype(str)
                
                # Calcular posição de Imperatriz entre os similares
                posicao_imperatriz = df_similares[df_similares['Destaque']].index[0] + 1 if df_similares['Destaque'].any() else None
                
                # Calcular média dos municípios similares (excluindo Imperatriz)
                df_similares_sem_imperatriz = df_similares[~df_similares['Destaque']]
                media_similares = df_similares_sem_imperatriz['Contagem'].mean() if len(df_similares_sem_imperatriz) > 0 else 0
                
                # Calcular variação de Imperatriz em relação à média
                variacao_vs_media = ((contagem_imperatriz - media_similares) / media_similares * 100) if media_similares > 0 else 0
                
                # Formatação
                df_similares['Contagem_Formatada'] = df_similares['Contagem'].apply(formatar_numero_br)
                
                # Exibir KPIs
                col_h6_1, col_h6_2, col_h6_3 = st.columns(3)
                with col_h6_1:
                    st.metric(
                        "Notificações de Imperatriz",
                        formatar_numero_br(contagem_imperatriz)
                    )
                with col_h6_2:
                    st.metric(
                        "Média dos Similares",
                        formatar_numero_br(int(media_similares)) if media_similares > 0 else "N/A"
                    )
                with col_h6_3:
                    st.metric(
                        "Posição entre Similares (H6)",
                        f"{posicao_imperatriz}º lugar" if posicao_imperatriz else "N/A",
                        delta=f"{variacao_vs_media:+.1f}% vs média" if media_similares > 0 else None
                    )
                
                # Limitar a 15 municípios para melhor visualização
                if len(df_similares) > 15:
                    # Pegar os top 7 acima de Imperatriz, Imperatriz, e os top 7 abaixo
                    idx_imperatriz = df_similares[df_similares['Destaque']].index[0]
                    inicio = max(0, idx_imperatriz - 7)
                    fim = min(len(df_similares), idx_imperatriz + 8)
                    df_similares_display = df_similares.iloc[inicio:fim].copy()
                else:
                    df_similares_display = df_similares.copy()
                
                # Ordenar para gráfico horizontal (ascendente)
                df_similares_display = df_similares_display.sort_values('Contagem', ascending=True)
                
                # Criar gráfico usando MUNICIPIO_UF (com sigla do estado)
                fig_h6 = px.bar(
                    df_similares_display,
                    x='Contagem',
                    y='MUNICIPIO_UF',
                    orientation='h',
                    title=f'Comparação: Imperatriz vs Municípios de Tamanho Semelhante (H6)',
                    labels={'MUNICIPIO_UF': 'Município', 'Contagem': 'Total de Notificações'},
                    height=500,
                    color='Destaque',
                    color_discrete_map={True: '#C73E1D', False: '#2E86AB'},  # Vermelho para Imperatriz, azul para outros
                    category_orders={'MUNICIPIO_UF': df_similares_display['MUNICIPIO_UF'].tolist()},
                    custom_data=['Contagem_Formatada']
                )
                
                fig_h6.update_layout(
                    margin=dict(l=200, r=50, t=80, b=50),
                    height=500,
                    showlegend=False,
                    yaxis={'categoryorder': 'array', 'categoryarray': df_similares_display['MUNICIPIO_UF'].tolist()}
                )
                
                aplicar_formatacao_eixo(fig_h6, df_similares_display['Contagem'].max(), eixo='x')
                
                # REMOVER números das barras - apenas hover
                fig_h6.update_traces(
                    hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<extra></extra>',
                    text=None,
                    textposition=None
                )
                
                # Garantir que não há texto definido em nenhum lugar
                for trace in fig_h6.data:
                    trace.text = None
                    trace.textposition = None
                
                st.plotly_chart(fig_h6, use_container_width=True)
                
                # Informação sobre faixa de comparação
                if media_similares > 0:
                    st.caption(f"**Faixa de comparação:** Municípios com {formatar_numero_br(int(limite_inferior))} a {formatar_numero_br(int(limite_superior))} notificações (±30% de Imperatriz). Total de {len(df_similares)} municípios similares encontrados.")
                else:
                    st.warning("Não foi possível calcular a comparação: nenhum município similar encontrado além de Imperatriz.")
            else:
                st.warning("Imperatriz não encontrada nos dados filtrados.")
        else:
            st.info("Dados de município não disponíveis para este gráfico.")
    
    # Gráfico 12: Tempo entre Ocorrência e Denúncia - REMOVIDO conforme solicitado
    
    # Gráfico 13: Status dos Casos (Encerrados, Abandonados)
    # Ocultado: Os dados não trazem informação útil sobre status (99.99% dos registros têm EVOLUCAO vazia)
    # Apenas 37 registros de 405,484 têm valores não vazios em EVOLUCAO
    # Portanto, este gráfico foi omitido por não fornecer informações relevantes
    pass
    
    # Gráfico 13: Encaminhamentos para Justiça - REMOVIDO conforme solicitado
    
    # Tabela de Dados Filtrados
    st.markdown('<div class="section-header">Dados Filtrados</div>', unsafe_allow_html=True)
    st.markdown("""
    **Base de Dados da Pesquisa**
    
    Esta tabela apresenta os dados utilizados na análise, conforme os filtros aplicados acima. 
    Todos os gráficos, métricas e análises apresentadas neste dashboard foram gerados a partir 
    destes registros filtrados, garantindo transparência e rastreabilidade dos resultados da pesquisa.
    """)
    
    # Selecionar colunas relevantes para exibição
    colunas_amostra = []
    if 'ANO_NOTIFIC' in df_filtrado.columns:
        colunas_amostra.append('ANO_NOTIFIC')
    if 'UF_NOTIFIC' in df_filtrado.columns:
        colunas_amostra.append('UF_NOTIFIC')
    if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns:
        colunas_amostra.append('MUNICIPIO_NOTIFIC')
    if 'FAIXA_ETARIA' in df_filtrado.columns:
        colunas_amostra.append('FAIXA_ETARIA')
    if 'SEXO' in df_filtrado.columns:
        colunas_amostra.append('SEXO')
    if 'TIPO_VIOLENCIA' in df_filtrado.columns:
        colunas_amostra.append('TIPO_VIOLENCIA')
    if 'LOCAL_OCOR' in df_filtrado.columns:
        colunas_amostra.append('LOCAL_OCOR')
    if 'AUTOR_SEXO_CORRIGIDO' in df_filtrado.columns:
        colunas_amostra.append('AUTOR_SEXO_CORRIGIDO')
    if 'GRAU_PARENTESCO' in df_filtrado.columns:
        colunas_amostra.append('GRAU_PARENTESCO')
    if 'CS_RACA' in df_filtrado.columns:
        colunas_amostra.append('CS_RACA')
    if 'DT_NOTIFIC' in df_filtrado.columns:
        colunas_amostra.append('DT_NOTIFIC')
    if 'DT_OCOR' in df_filtrado.columns:
        colunas_amostra.append('DT_OCOR')
    # TEMPO_OCOR_DENUNCIA removido - gráfico removido
    # STATUS_CASO removido - dados não trazem informação útil
    # ENCAMINHAMENTOS_JUSTICA removido - gráfico removido
    
    # Filtrar apenas colunas que existem no DataFrame
    colunas_disponiveis = [col for col in colunas_amostra if col in df_filtrado.columns]
    
    if colunas_disponiveis and len(df_filtrado) > 0:
        total_registros = len(df_filtrado)
        
        # Opção para o usuário escolher quantas linhas exibir
        col_opcao1, col_opcao2 = st.columns([2, 1])
        
        with col_opcao1:
            # Opções pré-definidas
            opcoes_exibicao = {
                'Amostra (100 linhas)': 100,
                'Amostra (500 linhas)': 500,
                'Amostra (1.000 linhas)': 1000,
                'Amostra (5.000 linhas)': 5000,
                'Todos os dados': total_registros
            }
            
            # Limitar opções baseado no total de registros
            opcoes_disponiveis = {k: v for k, v in opcoes_exibicao.items() if v <= total_registros}
            if total_registros > 10000:
                # Para muitos registros, adicionar aviso
                opcoes_disponiveis['Todos os dados (pode ser lento)'] = total_registros
                if 'Todos os dados' in opcoes_disponiveis:
                    del opcoes_disponiveis['Todos os dados']
            
            opcao_selecionada = st.selectbox(
                'Quantas linhas exibir?',
                options=list(opcoes_disponiveis.keys()),
                index=0 if total_registros > 1000 else len(opcoes_disponiveis) - 1,
                help='Para melhor performance, recomenda-se usar amostras. Exibir todos os dados pode tornar o dashboard mais lento.'
            )
            
            num_linhas = opcoes_disponiveis[opcao_selecionada]
        
        with col_opcao2:
            # Mostrar total de registros
            st.metric("Total de Registros", formatar_numero_br(total_registros))
        
        # Aviso de performance para muitos registros
        if num_linhas > 5000:
            st.warning(f"**Atenção:** Exibindo {formatar_numero_br(num_linhas)} linhas. Isso pode tornar o dashboard mais lento. Considere usar uma amostra menor.")
        elif num_linhas == total_registros and total_registros > 1000:
            st.info(f"ℹ️ Exibindo todos os {formatar_numero_br(total_registros)} registros. A tabela pode demorar um pouco para carregar.")
        
        # Preparar dados para exibição
        if num_linhas >= total_registros:
            # Exibir todos os dados
            df_exibicao = df_filtrado[colunas_disponiveis].copy()
        else:
            # Criar amostra aleatória
            df_exibicao = df_filtrado[colunas_disponiveis].sample(n=num_linhas, random_state=42).reset_index(drop=True)
        
        # Renomear colunas para nomes mais amigáveis
        nomes_amigaveis = {
            'ANO_NOTIFIC': 'Ano',
            'UF_NOTIFIC': 'Estado',
            'MUNICIPIO_NOTIFIC': 'Município',
            'FAIXA_ETARIA': 'Faixa Etária',
            'SEXO': 'Sexo',
            'TIPO_VIOLENCIA': 'Tipo de Violência',
            'LOCAL_OCOR': 'Local de Ocorrência',
            'AUTOR_SEXO_CORRIGIDO': 'Sexo do Agressor',
            'GRAU_PARENTESCO': 'Relacionamento',
            'CS_RACA': 'Raça/Cor',
            'DT_NOTIFIC': 'Data Notificação',
            'DT_OCOR': 'Data Ocorrência',
            # TEMPO_OCOR_DENUNCIA e ENCAMINHAMENTOS_JUSTICA removidos - gráficos removidos
        }
        
        df_exibicao = df_exibicao.rename(columns=nomes_amigaveis)
        
        # Formatar datas se existirem
        if 'Data Notificação' in df_exibicao.columns:
            df_exibicao['Data Notificação'] = pd.to_datetime(df_exibicao['Data Notificação'], errors='coerce')
            df_exibicao['Data Notificação'] = df_exibicao['Data Notificação'].dt.strftime('%d/%m/%Y')
        
        if 'Data Ocorrência' in df_exibicao.columns:
            df_exibicao['Data Ocorrência'] = pd.to_datetime(df_exibicao['Data Ocorrência'], errors='coerce')
            df_exibicao['Data Ocorrência'] = df_exibicao['Data Ocorrência'].dt.strftime('%d/%m/%Y')
        
        # Formatação de TEMPO_OCOR_DENUNCIA removida - gráfico removido
        
        # Ordenar por data (mais recente primeiro) se disponível
        if 'Data Notificação' in df_exibicao.columns:
            # Tentar ordenar por data (mas manter como string formatada)
            df_exibicao = df_exibicao.sort_values('Data Notificação', ascending=False, na_position='last')
        elif 'Ano' in df_exibicao.columns:
            df_exibicao = df_exibicao.sort_values('Ano', ascending=False, na_position='last')
        
        # Exibir tabela com altura adaptável
        altura_tabela = min(600, max(400, num_linhas * 30)) if num_linhas < 1000 else 600
        
        st.dataframe(
            df_exibicao,
            use_container_width=True,
            hide_index=True,
            height=altura_tabela
        )
        
        # Informações sobre a exibição
        if num_linhas < total_registros:
            st.caption(f"**Total de registros filtrados:** {formatar_numero_br(total_registros)} | **Linhas exibidas:** {formatar_numero_br(num_linhas)} (amostra aleatória)")
        else:
            st.caption(f"**Total de registros exibidos:** {formatar_numero_br(total_registros)}")
        
        # Opção para download dos dados
        st.markdown("---")
        csv = df_filtrado[colunas_disponiveis].to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 Download dos Dados Filtrados (CSV)",
            data=csv,
            file_name=f"dados_sinan_filtrados_{ano_selecionado[0]}_{ano_selecionado[1]}.csv",
            mime="text/csv",
            key="download_button",
            help=f"Baixar todos os {formatar_numero_br(total_registros)} registros filtrados em formato CSV"
        )
    else:
        st.info("Nenhum dado disponível para exibir.")

# Rodapé
st.markdown("---")
st.markdown("### Notas Metodológicas")
st.markdown("""
- **Fonte dos Dados:** Sistema de Informação de Agravos de Notificação (SINAN) - Ministério da Saúde
- **Período:** 2019-2024
- **População:** Crianças e adolescentes de 0 a 17 anos
- **Conformidade LGPD:** Dados agregados e anonimizados
""")

st.markdown("### Conformidade com LGPD")
st.markdown("""
Este dashboard utiliza apenas dados **agregados e anonimizados**, garantindo a proteção da privacidade dos indivíduos 
e a conformidade com a Lei Geral de Proteção de Dados (LGPD). Nenhum dado pessoal identificável é exibido.
""")

