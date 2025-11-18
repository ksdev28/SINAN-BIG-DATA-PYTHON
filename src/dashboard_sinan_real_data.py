
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
import plotly.graph_objects as go
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
    import duckdb
    DUCKDB_AVAILABLE = True
    from src.processors.sinan_data_processor_duckdb import SINANDataProcessorDuckDB
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

# Classe auxiliar para dados pré-processados (deve estar no nível do módulo para serialização)
class MinimalProcessor:
    """Processador mínimo para compatibilidade com dados pré-processados"""
    def __init__(self):
        self.dictionaries = {}

# Carregamento e Tratamento dos Dados
@st.cache_data(ttl=3600, max_entries=1)  # Limitar cache para evitar MemoryError
def load_sinan_data(use_duckdb=True, use_preprocessed=True):
    """
    Carrega dados reais do SINAN dos arquivos parquet (otimizado para grandes volumes)
    
    Args:
        use_duckdb: Se True e DuckDB disponível, usa DuckDB para melhor performance (fallback)
        use_preprocessed: Se True, tenta carregar dados pré-processados primeiro (mais rápido)
    """
    # Verificar se existem dados pré-processados
    if use_preprocessed:
        processed_file = project_root / "data" / "processed" / "sinan_data_processed.parquet"
        if processed_file.exists():
            try:
                print("[INFO] Carregando dados pre-processados...")
                df = pd.read_parquet(processed_file)
                print(f"[OK] Dados pre-processados carregados: {len(df):,} registros")
                
                # Verificar se as colunas essenciais existem
                colunas_essenciais = ['NU_IDADE_N', 'DT_NOTIFIC', 'CS_SEXO']
                colunas_faltando = [col for col in colunas_essenciais if col not in df.columns]
                if colunas_faltando:
                    print(f"[AVISO] Colunas faltando nos dados pre-processados: {colunas_faltando}")
                    print("[INFO] Continuando com processamento normal...")
                    raise ValueError(f"Colunas essenciais faltando: {colunas_faltando}")
                
                # Verificar se as colunas derivadas já existem
                colunas_derivadas = ['ANO_NOTIFIC', 'TIPO_VIOLENCIA', 'SEXO', 'UF_NOTIFIC', 'FAIXA_ETARIA']
                colunas_derivadas_faltando = [col for col in colunas_derivadas if col not in df.columns]
                
                if colunas_derivadas_faltando:
                    print(f"[INFO] Criando colunas derivadas faltantes: {colunas_derivadas_faltando}")
                    df = create_derived_columns(df)
                else:
                    print("[OK] Todas as colunas derivadas ja existem nos dados pre-processados")
                
                # Aplicar filtro de violência se necessário (os dados pré-processados podem não ter filtro)
                # Verificar se há registros sem violência marcada
                violencia_cols = ['VIOL_SEXU', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_INFAN']
                violencia_disponiveis = [col for col in violencia_cols if col in df.columns]
                
                if violencia_disponiveis:
                    # Verificar quantos registros têm violência marcada
                    def tem_violencia(row):
                        for col in violencia_disponiveis:
                            val = str(row.get(col, '')).upper().strip()
                            if val in ['1', 'SIM', 'S', '1.0']:
                                return True
                            if pd.notna(row.get(col)) and row[col] == 1:
                                return True
                        return False
                    
                    registros_com_violencia = df[df.apply(tem_violencia, axis=1)]
                    registros_sem_violencia = len(df) - len(registros_com_violencia)
                    
                    if registros_sem_violencia > 0:
                        print(f"[INFO] Aplicando filtro de violencia (removendo {registros_sem_violencia:,} registros sem violencia)...")
                        df = registros_com_violencia.copy()
                        print(f"[OK] {len(df):,} registros com violencia marcada")
                    else:
                        print("[OK] Todos os registros ja tem violencia marcada")
                
                # Criar um processador mínimo apenas para compatibilidade
                processor = MinimalProcessor()
                
                print("[OK] Dados pre-processados prontos para uso!")
                return df, processor
            except Exception as e:
                print(f"[ERRO] Erro ao carregar dados pre-processados: {e}")
                import traceback
                print(f"[DEBUG] Traceback completo:")
                traceback.print_exc()
                print("[INFO] Continuando com processamento normal...")
    
    try:
        # Usar DuckDB se disponível (muito mais rápido)
        if use_duckdb and DUCKDB_AVAILABLE:
            print("[INFO] Usando DuckDB para carregamento otimizado...")
            violence_path = project_root / "data" / "raw" / "VIOLBR-PARQUET"
            dict_path = project_root / "data" / "config" / "TAB_SINANONLINE"
            with SINANDataProcessorDuckDB(
                violence_data_path=str(violence_path),
                dict_path=str(dict_path)
            ) as processor_duckdb:
                # Carregar dicionários
                processor_duckdb.load_dictionaries()
                
                # Carregar dados filtrados diretamente (muito mais eficiente)
                child_data_raw = processor_duckdb.load_filtered_violence_data(
                    age_filter=True,
                    violence_filter=True
                )
                
                if child_data_raw is None or len(child_data_raw) == 0:
                    st.error("Nenhum dado encontrado nos arquivos parquet")
                    return None, None
                
                # Usar o processador original para aplicar dicionários
                processor = processor_duckdb.processor
                
                # Aplicar dicionários apenas nos dados filtrados
                decoded_data = processor.apply_dictionaries(child_data_raw)
                del child_data_raw
                
                # Preparar dados para visualização
                df = decoded_data.copy(deep=False)
                processor = processor_duckdb.processor
                # Pular o resto do processamento, já está feito
                skip_processing = True
        else:
            skip_processing = False
        
        if not skip_processing:
            # Fallback para método original
            print("Usando metodo tradicional (pandas)...")
            violence_path = project_root / "data" / "raw" / "VIOLBR-PARQUET"
            dict_path = project_root / "data" / "config" / "TAB_SINANONLINE"
            processor = SINANDataProcessorComprehensive(
                violence_data_path=str(violence_path),
                dict_path=str(dict_path)
            )
            
            # Carregar dicionários
            processor.load_dictionaries()
            
            # Carregar dados de violência
            violence_data = processor.load_violence_data()
            
            if violence_data is None or len(violence_data) == 0:
                st.error("Nenhum dado encontrado nos arquivos parquet")
                return None, None
            
            # OTIMIZAÇÃO: Filtrar por idade ANTES de aplicar dicionários
            # Isso reduz drasticamente o tamanho dos dados
            print("Filtrando por idade primeiro (0-17 anos)...")
            
            # Filtrar por código de idade diretamente (mais eficiente)
            if 'NU_IDADE_N' in violence_data.columns:
                # Códigos de idade: 4000 (menor de 1 ano) até 4017 (17 anos)
                # Formato: 4000, 4001, ..., 4009, 4010, 4011, ..., 4017
                age_codes = [f'400{i}' if i < 10 else f'40{i}' for i in range(0, 18)]
                
                # Converter para string e filtrar
                age_str = violence_data['NU_IDADE_N'].astype(str)
                age_filter = age_str.isin(age_codes)
                
                child_data_raw = violence_data[age_filter].copy(deep=False)
                
                filtered_count = len(child_data_raw)
                total_count = len(violence_data)
                percentage = (filtered_count/total_count*100) if total_count > 0 else 0
                
                print(f"   Dados filtrados: {filtered_count:,} registros de {total_count:,} ({percentage:.1f}%)")
                
                if filtered_count == 0:
                    print("   ATENCAO: Nenhum registro encontrado com codigos de idade 0-17 anos")
                    print(f"   Verificando valores unicos de idade (primeiros 20):")
                    unique_ages = violence_data['NU_IDADE_N'].value_counts().head(20)
                    print(f"   {unique_ages.to_dict()}")
                
                # Liberar memória do DataFrame original
                del violence_data
            else:
                print("   AVISO: Coluna NU_IDADE_N nao encontrada")
                child_data_raw = violence_data.copy(deep=False)
                del violence_data
            
            # Aplicar dicionários apenas nos dados filtrados (muito menor)
            # Processar apenas colunas essenciais para economizar memória
            essential_columns = [
                'DT_NOTIFIC', 'NU_ANO', 'SG_UF_NOT', 'SG_UF', 'ID_MUNICIP', 'ID_MN_RESI',
                'NU_IDADE_N', 'CS_SEXO', 'VIOL_FISIC', 'VIOL_PSICO', 'VIOL_SEXU', 'VIOL_INFAN',
                'LOCAL_OCOR', 'AUTOR_SEXO', 'AUTOR_ALCO', 'CS_ESCOL_N', 'CS_RACA'
            ]
            
            # Colunas adicionais importantes para análises avançadas
            additional_columns = [
                'DT_OCOR',      # Data de ocorrência (para calcular tempo até denúncia)
                'DT_ENCERRA',  # Data de encerramento (para status do caso)
                'DT_DIGITA',   # Data de digitação
                'DT_INVEST',   # Data de investigação
                'EVOLUCAO',    # Evolução do caso (encerrado, abandonado, etc.)
                'ENC_DELEG',   # Encaminhamento para delegacia
                'ENC_DPCA',    # Encaminhamento para DPCA (Delegacia de Proteção à Criança)
                'ENC_MPU',     # Encaminhamento para Ministério Público
                'ENC_VARA',    # Encaminhamento para Vara da Infância
                'DELEG',       # Delegacia
                'DELEG_CRIA',  # Delegacia da Criança
                'DELEG_IDOS',  # Delegacia do Idoso
                'DELEG_MULH',  # Delegacia da Mulher
                'HORA_OCOR',   # Hora da ocorrência
                'CLASSI_FIN'   # Classificação final
            ]
            
            # Manter apenas colunas essenciais + todas as colunas REL_ e outras importantes
            available_essential = [col for col in essential_columns if col in child_data_raw.columns]
            available_additional = [col for col in additional_columns if col in child_data_raw.columns]
            rel_cols = [col for col in child_data_raw.columns if col.startswith('REL_')]
            other_cols = [col for col in child_data_raw.columns if col in ['SIT_CONJUG', 'ID_OCUPA_N', 'REDE_SAU', 'REDE_EDUCA']]
            
            columns_to_keep = list(set(available_essential + available_additional + rel_cols + other_cols))
            child_data_subset = child_data_raw[columns_to_keep].copy(deep=False)
            
            decoded_data = processor.apply_dictionaries(child_data_subset)
            
            # Liberar memória após processar
            del child_data_raw
            del child_data_subset
            
            # Filtrar violência contra crianças e adolescentes (0-17 anos)
            # Agora já está filtrado por idade, só precisa verificar tipos de violência
            child_data = processor.filter_comprehensive_violence(decoded_data, already_filtered_by_age=True)
            
            # Preparar dados para visualização (usar shallow copy)
            df = child_data.copy(deep=False)
        
        # Aplicar transformações de colunas derivadas
        df = create_derived_columns(df)
        
        return df, processor
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None, None

def create_derived_columns(df):
    """
    Cria todas as colunas derivadas necessárias para o dashboard
    Esta função é chamada tanto para dados pré-processados quanto para dados processados normalmente
    """
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
            except Exception as e:
                try:
                    # Tentar outros formatos
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                except:
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

# Carregar dados
with st.spinner("Carregando dados do SINAN..."):
    try:
        df, processor = load_sinan_data()
    except MemoryError:
        # Se houver erro de memória, limpar cache e tentar novamente
        st.cache_data.clear()
        st.warning("⚠️ Cache limpo devido a erro de memória. Recarregando dados...")
        df, processor = load_sinan_data()

if df is None or len(df) == 0:
    st.error("Não foi possível carregar os dados. Verifique se os arquivos parquet estão disponíveis.")
    st.stop()

# Seção de Diagnóstico removida conforme solicitado

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

# Filtros (Interatividade)
st.sidebar.header("Filtros de Análise")

# Filtro de Ano
if 'ANO_NOTIFIC' in df.columns and df['ANO_NOTIFIC'].notna().any():
    anos_disponiveis = sorted(df['ANO_NOTIFIC'].dropna().unique())
    if anos_disponiveis:
        ano_min = int(min(anos_disponiveis))
        ano_max = int(max(anos_disponiveis))
        ano_selecionado = st.sidebar.slider(
            'Selecione o Período (Anos)',
            min_value=ano_min,
            max_value=ano_max,
            value=(ano_min, ano_max)
        )
    else:
        ano_selecionado = (2019, 2024)
        st.sidebar.warning("Anos não disponíveis nos dados")
else:
    ano_selecionado = (2019, 2024)
    st.sidebar.warning("Coluna de ano não encontrada")

# Filtro de Região (UF)
if 'UF_NOTIFIC' in df.columns:
    uf_options = ['Todos'] + sorted([uf for uf in df['UF_NOTIFIC'].dropna().unique() if str(uf) != 'nan' and str(uf) != 'N/A'])
    uf_selecionada = st.sidebar.selectbox('Filtrar por UF', uf_options, index=0)
else:
    uf_selecionada = 'Todos'

# Aplicar filtros
df_filtrado = df.copy()

# Filtro de ano
if 'ANO_NOTIFIC' in df_filtrado.columns:
    df_filtrado = df_filtrado[
        (df_filtrado['ANO_NOTIFIC'] >= ano_selecionado[0]) & 
        (df_filtrado['ANO_NOTIFIC'] <= ano_selecionado[1])
    ]

# Filtro de UF
if uf_selecionada != 'Todos':
    df_filtrado = df_filtrado[df_filtrado['UF_NOTIFIC'] == uf_selecionada]
    
    # Filtro de Município (Apenas se a UF for selecionada)
    if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns:
        # Buscar todos os municípios da UF selecionada (sem limite)
        municipios_disponiveis = sorted([m for m in df_filtrado['MUNICIPIO_NOTIFIC'].dropna().unique() 
                                         if str(m) != 'nan' and str(m) != 'N/A'])
        if municipios_disponiveis:
            municipio_options = ['Todos'] + municipios_disponiveis
            municipio_selecionado = st.sidebar.selectbox(
                f'Filtrar por Município ({len(municipios_disponiveis)} disponíveis)', 
                municipio_options, 
                index=0
            )
            
            if municipio_selecionado != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['MUNICIPIO_NOTIFIC'] == municipio_selecionado]
        else:
            municipio_selecionado = 'Todos'
    else:
        municipio_selecionado = 'Todos'
else:
    municipio_selecionado = 'Todos'

# Filtro de Tipo de Violência
if 'TIPO_VIOLENCIA' in df_filtrado.columns:
    tipos_disponiveis = sorted(df_filtrado['TIPO_VIOLENCIA'].dropna().unique())
    if tipos_disponiveis:
        tipo_options = ['Todos'] + tipos_disponiveis
        tipo_selecionado = st.sidebar.selectbox('Filtrar por Tipo de Violência', tipo_options, index=0)
        
        if tipo_selecionado != 'Todos':
            df_filtrado = df_filtrado[df_filtrado['TIPO_VIOLENCIA'].str.contains(tipo_selecionado, na=False)]
    else:
        tipo_selecionado = 'Todos'
else:
    tipo_selecionado = 'Todos'

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
    contexto_analise = "**Análise:** Brasil (Todos os Estados)"

# Gráficos
if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    # Contexto da análise atual
    total_formatado = formatar_numero_br(total_notificacoes)
    st.info(f"**Análise atual:** {contexto_analise} | **Período:** {ano_selecionado[0]}-{ano_selecionado[1]} | **Total de registros:** {total_formatado}")
    
    # Gráfico 1: Tendência Temporal (H1, H10)
    st.markdown('<div class="section-header">1. Tendência de Notificações ao Longo dos Anos (H1, H10)</div>', unsafe_allow_html=True)
    st.markdown("**Hipóteses H1 e H10:** Verificar a tendência temporal e evolução das notificações ao longo dos anos.")
    
    if 'ANO_NOTIFIC' in df_filtrado.columns and df_filtrado['ANO_NOTIFIC'].notna().any():
        df_tendencia = df_filtrado.groupby('ANO_NOTIFIC').size().reset_index(name='Total_Notificacoes')
        df_tendencia = df_tendencia.sort_values('ANO_NOTIFIC')
        df_tendencia['Total_Formatado'] = df_tendencia['Total_Notificacoes'].apply(formatar_numero_br)
        
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
        fig_line.update_traces(
            customdata=df_tendencia['Total_Formatado'],
            hovertemplate='<b>%{x}</b><br>Total: %{customdata}<extra></extra>'
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Dados de ano não disponíveis para este gráfico")

    # Gráfico 2: Composição por Tipo de Violência (H9)
    st.markdown('<div class="section-header">2. Composição Anual por Tipo de Violência (H9)</div>', unsafe_allow_html=True)
    st.markdown("**Hipótese H9:** Analisar a composição e proporção de cada tipo de violência ao longo dos anos.")
    
    if 'TIPO_VIOLENCIA' in df_filtrado.columns and 'ANO_NOTIFIC' in df_filtrado.columns:
        df_composicao = df_filtrado.groupby(['ANO_NOTIFIC', 'TIPO_VIOLENCIA']).size().reset_index(name='Contagem')
        df_composicao = df_composicao.sort_values('ANO_NOTIFIC')
        
        # Pegar apenas os tipos mais frequentes para não sobrecarregar o gráfico
        tipos_mais_freq = df_filtrado['TIPO_VIOLENCIA'].value_counts().head(5).index.tolist()
        df_composicao_filtrado = df_composicao[df_composicao['TIPO_VIOLENCIA'].isin(tipos_mais_freq)]
        df_composicao_filtrado['Contagem_Formatada'] = df_composicao_filtrado['Contagem'].apply(formatar_numero_br)
        
        fig_bar_stacked = px.bar(
            df_composicao_filtrado, 
            x='ANO_NOTIFIC', 
            y='Contagem', 
            color='TIPO_VIOLENCIA',
            title='Notificações por Ano e Tipo de Violência',
            labels={'ANO_NOTIFIC': 'Ano', 'Contagem': 'Contagem de Notificações', 'TIPO_VIOLENCIA': 'Tipo de Violência'},
            barmode='stack',
            color_discrete_sequence=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', '#BC4749', '#7209B7', '#FF6B35', '#4ECDC4', '#FFE66D'],  # Cores diversas e bem visíveis
            height=500,  # Altura fixa para evitar compressão
            custom_data=['Contagem_Formatada', 'TIPO_VIOLENCIA']
        )
        # Melhorar layout: legenda abaixo com mais espaçamento do gráfico
        fig_bar_stacked.update_layout(
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.25,  # Aumentado para mais espaçamento entre gráfico e legenda
                xanchor="center",
                x=0.5,
                font=dict(size=10),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.2)",
                borderwidth=1,
                itemwidth=30,
                tracegroupgap=10
            ),
            margin=dict(l=50, r=50, t=80, b=120),  # Aumentada margem inferior para acomodar legenda
            height=500
        )
        max_contagem = df_composicao_filtrado['Contagem'].max()
        aplicar_formatacao_eixo(fig_bar_stacked, max_contagem, eixo='y')
        fig_bar_stacked.update_traces(
            hovertemplate='<b>%{x}</b><br>Tipo: %{customdata[1]}<br>Total: %{customdata[0]}<extra></extra>'
        )
        st.plotly_chart(fig_bar_stacked, use_container_width=True)
    else:
        st.info("Dados de tipo de violência ou ano não disponíveis para este gráfico")

    # Gráfico 3: Distribuição por Faixa Etária e Sexo (H2, H4, H5)
    st.markdown('<div class="section-header">3. Distribuição por Faixa Etária e Sexo (H2, H4, H5)</div>', unsafe_allow_html=True)
    st.markdown("**Hipóteses H2, H4 e H5:** Comparar a incidência entre diferentes faixas etárias e sexo da vítima.")
    st.info("**Nota:** Análise para crianças e adolescentes de **0 a 17 anos** (faixas: 0-1, 2-5, 6-9, 10-13, 14-17 anos).")
    
    if 'FAIXA_ETARIA' in df_filtrado.columns and 'SEXO' in df_filtrado.columns:
        # Garantir que apenas faixas etárias de 0-17 anos sejam incluídas
        faixas_validas = ['0-1 anos', '2-5 anos', '6-9 anos', '10-13 anos', '14-17 anos']
        
        # Filtrar apenas registros com faixas válidas e sexo válido
        df_demografia = df_filtrado[
            (df_filtrado['FAIXA_ETARIA'].isin(faixas_validas)) & 
            (df_filtrado['SEXO'].notna()) & 
            (df_filtrado['SEXO'] != 'Não informado')
        ].copy()
        
        if len(df_demografia) > 0:
            df_demografia = df_demografia.groupby(['FAIXA_ETARIA', 'SEXO']).size().reset_index(name='Contagem')
            
            # Ordenar faixas etárias na ordem correta (0-1, 2-5, 6-9, 10-13, 14-17)
            ordem_faixas = {faixa: idx for idx, faixa in enumerate(faixas_validas)}
            df_demografia['ordem'] = df_demografia['FAIXA_ETARIA'].map(ordem_faixas)
            df_demografia = df_demografia.sort_values('ordem').drop('ordem', axis=1)
            
            # Garantir que todas as faixas apareçam no gráfico, mesmo com 0 registros
            # Criar DataFrame completo com todas as combinações
            import itertools
            todas_combinacoes = list(itertools.product(faixas_validas, df_demografia['SEXO'].unique()))
            df_completo = pd.DataFrame(todas_combinacoes, columns=['FAIXA_ETARIA', 'SEXO'])
            df_demografia = df_completo.merge(df_demografia, on=['FAIXA_ETARIA', 'SEXO'], how='left')
            df_demografia['Contagem'] = df_demografia['Contagem'].fillna(0).astype(int)
            df_demografia['Contagem_Formatada'] = df_demografia['Contagem'].apply(formatar_numero_br)
            
            fig_bar_grouped = px.bar(
                df_demografia, 
                x='FAIXA_ETARIA', 
                y='Contagem', 
                color='SEXO',
                barmode='group',
                title='Contagem de Notificações por Faixa Etária e Sexo (0-17 anos)',
                labels={'FAIXA_ETARIA': 'Faixa Etária', 'Contagem': 'Contagem de Notificações', 'SEXO': 'Sexo'},
                category_orders={'FAIXA_ETARIA': faixas_validas},  # Garantir ordem correta
                color_discrete_sequence=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'],  # Cores escuras e visíveis
                height=500,  # Altura fixa
                custom_data=['Contagem_Formatada', 'SEXO']
            )
            fig_bar_grouped.update_xaxes(tickangle=0)
            # Melhorar layout: legenda horizontal abaixo com mais espaçamento do gráfico
            fig_bar_grouped.update_layout(
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.25,  # Aumentado para mais espaçamento entre gráfico e legenda
                    xanchor="center",
                    x=0.5,
                    font=dict(size=11),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.2)",
                    borderwidth=1,
                    itemwidth=40,
                    tracegroupgap=20,
                    itemsizing="constant",
                    itemclick="toggleothers",
                    itemdoubleclick="toggle"
                ),
                margin=dict(l=50, r=50, t=80, b=120),  # Aumentada margem inferior para acomodar legenda
                height=500
            )
            max_contagem_demografia = df_demografia['Contagem'].max()
            aplicar_formatacao_eixo(fig_bar_grouped, max_contagem_demografia, eixo='y')
            fig_bar_grouped.update_traces(
                hovertemplate='<b>%{x}</b><br>Sexo: %{customdata[1]}<br>Total: %{customdata[0]}<extra></extra>'
            )
            st.plotly_chart(fig_bar_grouped, use_container_width=True)
            
            # Mostrar estatísticas
            total_casos = df_demografia['Contagem'].sum()
            st.caption(f"**Total de casos analisados:** {formatar_numero_br(total_casos)} | **Faixas etárias:** {', '.join(faixas_validas)}")
        else:
            st.warning("Nenhum dado encontrado nas faixas etárias de 0-17 anos para os filtros selecionados.")
    else:
        st.info("Dados demográficos não disponíveis para este gráfico")

    # Gráfico 4: Distribuição Geográfica (H6, H7)
    if uf_selecionada == 'Todos':
        st.markdown('<div class="section-header">4. Distribuição Geográfica por UF (H7)</div>', unsafe_allow_html=True)
        st.markdown("**(Gráfico de Barras)**: Comparação entre Unidades Federativas.")
        
        if 'UF_NOTIFIC' in df_filtrado.columns:
            df_geo = df_filtrado.groupby('UF_NOTIFIC').size().reset_index(name='Contagem')
            df_geo = df_geo.sort_values('Contagem', ascending=False).head(27)  # Top 27 UFs
            df_geo['Contagem_Formatada'] = df_geo['Contagem'].apply(formatar_numero_br)
            
            fig_geo = px.bar(
                df_geo,
                x='UF_NOTIFIC',
                y='Contagem',
                title='Contagem de Notificações por UF',
                labels={'UF_NOTIFIC': 'Unidade Federativa', 'Contagem': 'Contagem de Notificações'},
                height=500,
                custom_data=['Contagem_Formatada']
            )
            fig_geo.update_xaxes(tickangle=45)
            fig_geo.update_layout(
                margin=dict(l=50, r=50, t=80, b=100),
                height=500
            )
            aplicar_formatacao_eixo(fig_geo, df_geo['Contagem'].max(), eixo='y')
            fig_geo.update_traces(
                hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
            )
            st.plotly_chart(fig_geo, use_container_width=True)
        else:
            st.info("Dados geográficos não disponíveis para este gráfico")
    
    elif municipio_selecionado == 'Todos' and uf_selecionada != 'Todos':
        st.markdown('<div class="section-header">4. Distribuição Geográfica por Município (H6)</div>', unsafe_allow_html=True)
        st.markdown("**(Gráfico de Barras)**: Comparação entre municípios.")
        
        if 'MUNICIPIO_NOTIFIC' in df_filtrado.columns:
            df_municipio = df_filtrado.groupby('MUNICIPIO_NOTIFIC').size().reset_index(name='Contagem')
            df_municipio = df_municipio.sort_values('Contagem', ascending=False).head(20)  # Top 20 municípios
            df_municipio['Contagem_Formatada'] = df_municipio['Contagem'].apply(formatar_numero_br)
            
            fig_mun_bar = px.bar(
                df_municipio,
                x='MUNICIPIO_NOTIFIC',
                y='Contagem',
                title=f'Top 20 Municípios com Mais Notificações ({uf_selecionada})',
                labels={'MUNICIPIO_NOTIFIC': 'Município', 'Contagem': 'Contagem de Notificações'},
                height=500,
                custom_data=['Contagem_Formatada']
            )
            fig_mun_bar.update_xaxes(tickangle=45)
            fig_mun_bar.update_layout(
                margin=dict(l=50, r=50, t=80, b=150),  # Mais espaço para rótulos inclinados
                height=500
            )
            aplicar_formatacao_eixo(fig_mun_bar, df_municipio['Contagem'].max(), eixo='y')
            fig_mun_bar.update_traces(
                hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
            )
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
            hovertemplate='<b>%{y}</b><br>Total: %{customdata[0]}<extra></extra>'
        )
        st.plotly_chart(fig_local, use_container_width=True)

    # Gráfico 6: Perfil do Agressor (Sexo) - H3
    if 'AUTOR_SEXO_CORRIGIDO' in df_filtrado.columns:
        st.markdown('<div class="section-header">6. Perfil do Agressor - Sexo (H3)</div>', unsafe_allow_html=True)
        st.markdown("**Hipótese H3:** Verificar a distribuição por sexo do agressor.")
        
        df_autor = df_filtrado['AUTOR_SEXO_CORRIGIDO'].value_counts().reset_index()
        df_autor.columns = ['Sexo', 'Contagem']
        df_autor = df_autor[df_autor['Sexo'] != 'Não informado']  # Filtrar valores inválidos
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
            custom_data=['Contagem_Formatada']
        )
        fig_autor.update_layout(
            showlegend=False,  # Não precisa de legenda (já está nas barras)
            margin=dict(l=50, r=50, t=80, b=50),
            height=450
        )
        aplicar_formatacao_eixo(fig_autor, df_autor['Contagem'].max(), eixo='y')
        fig_autor.update_traces(
            hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
        )
        st.plotly_chart(fig_autor, use_container_width=True)
    
    # Gráfico 7: Relacionamento com o Agressor - H8 (Apenas os mais comuns)
    if 'GRAU_PARENTESCO' in df_filtrado.columns:
        st.markdown('<div class="section-header">7. Relacionamento com o Agressor (H8)</div>', unsafe_allow_html=True)
        st.markdown("**Hipótese H8:** Analisar a relação entre vítima e agressor. Exibindo apenas os relacionamentos mais comuns.")
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
                custom_data=['Contagem_Formatada']
            )
            # Formatação brasileira: ponto para milhares
            fig_parent.update_traces(
                texttemplate='%{text:,.0f}'.replace(',', 'X').replace('.', ',').replace('X', '.'),
                textposition='outside'
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
        st.markdown("**Hipótese H4:** Verificar a distribuição por raça/cor da vítima.")
        
        df_raca = df_filtrado['CS_RACA'].value_counts().reset_index()
        df_raca.columns = ['Raça/Cor', 'Contagem']
        df_raca = df_raca[~df_raca['Raça/Cor'].astype(str).str.contains('Ignorado|Branco|Não informado', case=False, na=False)]
        df_raca = df_raca.sort_values('Contagem', ascending=False)
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
                custom_data=['Contagem_Formatada']
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
                hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
            )
            st.plotly_chart(fig_raca, use_container_width=True)
    
    # Gráfico 9: Evolução Mensal - H10
    if 'DT_NOTIFIC' in df_filtrado.columns and df_filtrado['DT_NOTIFIC'].notna().any():
        st.markdown('<div class="section-header">9. Evolução Mensal de Notificações (H10)</div>', unsafe_allow_html=True)
        st.markdown("**Hipótese H10:** Analisar a evolução mensal das notificações.")
        
        df_filtrado['MES_ANO'] = df_filtrado['DT_NOTIFIC'].dt.to_period('M').astype(str)
        df_mensal = df_filtrado.groupby('MES_ANO').size().reset_index(name='Total')
        df_mensal = df_mensal.sort_values('MES_ANO')
        df_mensal['Total_Formatado'] = df_mensal['Total'].apply(formatar_numero_br)
        
        fig_mensal = px.line(
            df_mensal,
            x='MES_ANO',
            y='Total',
            markers=True,
            title='Evolução Mensal de Notificações (H10)',
            labels={'MES_ANO': 'Mês/Ano', 'Total': 'Total de Notificações'},
            color_discrete_sequence=['#1a237e'],  # Cor mais escura e visível
            height=500
        )
        fig_mensal.update_xaxes(tickangle=45, nticks=20)
        fig_mensal.update_layout(
            margin=dict(l=50, r=50, t=80, b=150),  # Mais espaço para rótulos inclinados
            height=500
        )
        aplicar_formatacao_eixo(fig_mensal, df_mensal['Total'].max(), eixo='y')
        fig_mensal.update_traces(
            customdata=df_mensal['Total_Formatado'],
            hovertemplate='<b>%{x}</b><br>Total: %{customdata}<extra></extra>'
        )
        st.plotly_chart(fig_mensal, use_container_width=True)
    
    # Gráfico 11: Sazonalidade - REMOVIDO conforme solicitado
    
    # Gráfico 10: Comparação Regional - H6, H7
    if uf_selecionada == 'Todos':
        st.markdown('<div class="section-header">10. Comparação Regional - Top 10 Estados (H6, H7)</div>', unsafe_allow_html=True)
        st.markdown("**Hipóteses H6 e H7:** Comparar a incidência entre diferentes regiões do país.")
        
        if 'UF_NOTIFIC' in df_filtrado.columns:
            df_regional = df_filtrado.groupby('UF_NOTIFIC').size().reset_index(name='Contagem')
            df_regional = df_regional.sort_values('Contagem', ascending=False).head(10)
            df_regional['Contagem_Formatada'] = df_regional['Contagem'].apply(formatar_numero_br)
            
            fig_regional = px.bar(
                df_regional,
                x='UF_NOTIFIC',
                y='Contagem',
                title='Top 10 Estados por Número de Notificações (H6, H7)',
                labels={'UF_NOTIFIC': 'Estado', 'Contagem': 'Total de Notificações'},
                color='Contagem',
                color_continuous_scale='Magma',  # Paleta mais escura e visível
                height=500,
                custom_data=['Contagem_Formatada']
            )
            fig_regional.update_xaxes(tickangle=45)
            # Layout responsivo: colorbar na lateral (desktop) por padrão
            # JavaScript ajustará para abaixo em mobile
            fig_regional.update_layout(
                margin=dict(l=50, r=80, t=80, b=50),  # Espaço para colorbar lateral (desktop)
                height=500,
                coloraxis_showscale=True,
                coloraxis_colorbar=get_colorbar_layout(is_mobile=False)  # Desktop por padrão
            )
            aplicar_formatacao_eixo(fig_regional, df_regional['Contagem'].max(), eixo='y')
            fig_regional.update_traces(
                hovertemplate='<b>%{x}</b><br>Total: %{customdata[0]}<extra></extra>'
            )
            st.plotly_chart(fig_regional, use_container_width=True)
    
    # Gráfico 12: Tempo entre Ocorrência e Denúncia - REMOVIDO conforme solicitado
    
    # Gráfico 13: Status dos Casos (Encerrados, Abandonados)
    # Ocultado: Os dados não trazem informação útil sobre status (99.99% dos registros têm EVOLUCAO vazia)
    # Apenas 37 registros de 405,484 têm valores não vazios em EVOLUCAO
    # Portanto, este gráfico foi omitido por não fornecer informações relevantes
    pass
    
    # Gráfico 13: Encaminhamentos para Justiça - REMOVIDO conforme solicitado
    
    # Tabela de Dados Filtrados
    st.markdown('<div class="section-header">Dados Filtrados</div>', unsafe_allow_html=True)
    st.markdown("**Dados conforme os filtros aplicados.**")
    
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
            st.warning(f"⚠️ **Atenção:** Exibindo {formatar_numero_br(num_linhas)} linhas. Isso pode tornar o dashboard mais lento. Considere usar uma amostra menor.")
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

