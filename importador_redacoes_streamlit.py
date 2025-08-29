# -*- coding: utf-8 -*-
"""
Importador de Redações (Planilha -> MySQL)

▶ O que faz
- Permite ao usuário anexar uma planilha com as colunas:
  redacao_id, arquivo_nome_armazenamento, tema, redacao_texto, co_redacao_grade_id
- Limpa a tabela `temp_analise_correcao_humano` e insere os novos registros
- Define automaticamente: corretor='IA' e nota_c1..nota_c5 = 0
- Mostra progresso da importação e resumo final

▶ Requisitos
pip install streamlit pandas SQLAlchemy pymysql python-dotenv

▶ Execução
streamlit run importador_redacoes_streamlit.py

▶ Configuração (.env)
DB_CONNECTION=mysql
DB_HOST=[meu_host_db]
DB_PORT=3306
DB_DATABASE=corrigeai
DB_USERNAME=udb
DB_PASSWORD=[minha_senha_db]

Observação: credenciais são carregadas do .env e **não** aparecem na interface.
"""

import os
from io import BytesIO
from typing import Tuple, List, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Tolerante se python-dotenv não estiver instalado
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*args, **kwargs):
        return False

# ===================== Configuração de Página (UI) =====================
st.set_page_config(
    page_title="CorreigeAI • Importador de Redações",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ===================== Estilos (UI) =====================
CUSTOM_CSS = """
<style>
/* Layout geral */
.block-container { padding-top: 1rem; padding-bottom: 2rem; }
header[data-testid="stHeader"] { backdrop-filter: blur(6px); }

/********* Cartões *********/
.card { background: #ffffff; border-radius: 14px; box-shadow: 0 6px 24px rgba(0,0,0,0.06); padding: 1.25rem 1.25rem; border: 1px solid #ececec; }
.card h3 { margin-top: 0; margin-bottom: .75rem; }
.small { color: #666; font-size: 0.92rem; }
.badge { display:inline-block; padding: .2rem .55rem; border-radius: 10px; background: #f1f5f9; margin-left: .35rem; font-size: .8rem; color:#334155; }

/********* Botões *********/
.stButton>button { border-radius: 999px; padding: .55rem 1rem; font-weight: 600; }
.stButton>button[kind="secondary"] { background: #f8fafc; border: 1px solid #e2e8f0; }

/********* Progresso *********/
.progress-wrap { background: #f1f5f9; border-radius: 999px; height: 12px; position: relative; overflow: hidden; border: 1px solid #e2e8f0; }
.progress-bar { height: 100%; width: 0%; background: linear-gradient(90deg,#60a5fa,#22c55e); transition: width .25s ease; }
.progress-label { font-size: 0.85rem; color: #334155; margin-top: .35rem; }

/********* Tabelas *********/
.dataframe tbody tr:hover { background: #f8fafc; }

</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ===================== Utilidades =====================
def _clean_env(s: str | None) -> str:
    """Remove colchetes/aspas acidentais do .env (ex.: ["usuario"] -> usuario)."""
    if s is None:
        return ""
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return s


def build_engine() -> Tuple[Engine | None, str | None]:
    """Cria Engine do SQLAlchemy a partir das variáveis de ambiente. Não expõe credenciais em mensagens de erro."""
    load_dotenv()
    db_host = _clean_env(os.getenv("DB_HOST"))
    db_port = _clean_env(os.getenv("DB_PORT", "3306"))
    db_name = _clean_env(os.getenv("DB_DATABASE"))
    db_user = _clean_env(os.getenv("DB_USERNAME"))
    db_pass = _clean_env(os.getenv("DB_PASSWORD"))

    missing = [k for k, v in {
        "DB_HOST": db_host,
        "DB_PORT": db_port,
        "DB_DATABASE": db_name,
        "DB_USERNAME": db_user,
        "DB_PASSWORD": db_pass,
    }.items() if not v]
    if missing:
        return None, f"Variáveis ausentes no .env: {', '.join(missing)}"

    uri = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    try:
        engine = create_engine(uri, pool_pre_ping=True, pool_recycle=3600, future=True)
        # Testa conexão rápida
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine, None
    except SQLAlchemyError as e:
        return None, "Não foi possível conectar ao banco MySQL. Verifique host, porta e permissões."


def read_uploaded_file(file) -> pd.DataFrame:
    """Lê CSV/XLSX do uploader para DataFrame. Normaliza nomes de colunas."""
    name = (file.name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(file)
        else:
            # Padrão: Excel
            df = pd.read_excel(file)
    except Exception as e:
        raise RuntimeError("Falha ao ler a planilha. Verifique formato e conteúdo.") from e

    # Normaliza colunas: lower, strip, troca espaços por underscore
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


REQUIRED_COLUMNS = [
    "redacao_id",
    "arquivo_nome_armazenamento",
    "tema",
    "redacao_texto",
    "co_redacao_grade_id",
]

INSERT_COLUMNS = [
    "redacao_id",
    "corretor",
    "situacao_nota_zero",
    "nota_c1",
    "nota_c2",
    "nota_c3",
    "nota_c4",
    "nota_c5",
    "arquivo_nome_armazenamento",
    "tema",
    "redacao_texto",
    "co_redacao_grade_id",
    "ocr_confianca",
    "arquivo_anonimo_nome_armazenamento",
]

# ===================== Layout =====================
st.title("📝 Importador de Redações para CorreigeAI")
st.caption("Interface de importação segura. As credenciais são internas ao sistema.")

with st.container():
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### 1) Anexe a planilha de redações - Formatos aceitos: **.xlsx** ou **.csv** - Colunas obrigatórias: `redacao_id`, `arquivo_nome_armazenamento`, `tema`, `redacao_texto`, `co_redacao_grade_id`", unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Selecionar arquivo", type=["xlsx", "xls", "csv"], accept_multiple_files=False)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown("### Status do Banco <span class='badge'>MySQL</span>", unsafe_allow_html=True)
        engine, err = build_engine()
        if err:
            st.error(err)
        else:
            st.success("Conexão ativa com o banco de dados.")
        st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="card">', unsafe_allow_html=True)
st.markdown("### 2) Importação")
importar = st.button("🚀 Importar para o banco", type="primary", disabled=uploaded_file is None or engine is None)

# ===================== Ação: Importar =====================
if importar:
    assert engine is not None, "Engine não pode ser None aqui."
    if uploaded_file is None:
        st.warning("Anexe um arquivo antes de importar.")
        st.stop()

    try:
        df = read_uploaded_file(uploaded_file)
    except RuntimeError as e:
        st.error(str(e))
        st.stop()

    # Validação de colunas obrigatórias
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        st.error(f"A planilha está sem as colunas obrigatórias: {', '.join(missing_cols)}")
        st.stop()

    # Sanitizações básicas
    df = df.copy()
    df = df.dropna(subset=["redacao_id"])  # precisa de id
    df["redacao_id"] = pd.to_numeric(df["redacao_id"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["redacao_id"])  # remove ids inválidos
    df["redacao_texto"] = df["redacao_texto"].fillna("").astype(str)
    df["tema"] = df["tema"].fillna("").astype(str)
    df["arquivo_nome_armazenamento"] = df["arquivo_nome_armazenamento"].fillna("").astype(str)
    df["co_redacao_grade_id"] = pd.to_numeric(df["co_redacao_grade_id"], errors="coerce").astype("Int64")

    # Deduplicação por redacao_id (mantém a última ocorrência)
    df = df.drop_duplicates(subset=["redacao_id"], keep="last")

    # Adiciona campos fixos/padrões
    df["corretor"] = "IA"
    for n in (1, 2, 3, 4, 5):
        df[f"nota_c{n}"] = 0
    df["situacao_nota_zero"] = None
    df["ocr_confianca"] = None
    df["arquivo_anonimo_nome_armazenamento"] = None

    # Reordena para o INSERT
    df_insert = df[INSERT_COLUMNS].copy()

    # Confirmação (contagem)
    total = len(df_insert)
    if total == 0:
        st.warning("Nada para importar após validação.")
        st.stop()

    # Apaga registros existentes
    st.info("Limpando tabela de destino…")
    try:
        with engine.begin() as conn:
            try:
                conn.execute(text("TRUNCATE TABLE temp_analise_correcao_humano"))
            except SQLAlchemyError:
                # Fallback caso TRUNCATE não seja permitido
                conn.execute(text("DELETE FROM temp_analise_correcao_humano"))
    except SQLAlchemyError as e:
        st.error("Falha ao limpar a tabela de destino. Verifique permissões de TRUNCATE/DELETE.")
        st.stop()

    # Inserção em lotes com barra de progresso
    st.info("Inserindo novos registros…")
    progress_placeholder = st.empty()
    bar_html = "<div class='progress-wrap'><div class='progress-bar' style='width:0%'></div></div><div class='progress-label'>0%</div>"
    progress_placeholder.markdown(bar_html, unsafe_allow_html=True)

    insert_sql = text(
        """
        INSERT INTO temp_analise_correcao_humano (
            redacao_id, corretor, situacao_nota_zero,
            nota_c1, nota_c2, nota_c3, nota_c4, nota_c5,
            arquivo_nome_armazenamento, tema, redacao_texto, co_redacao_grade_id,
            ocr_confianca, arquivo_anonimo_nome_armazenamento
        ) VALUES (
            :redacao_id, :corretor, :situacao_nota_zero,
            :nota_c1, :nota_c2, :nota_c3, :nota_c4, :nota_c5,
            :arquivo_nome_armazenamento, :tema, :redacao_texto, :co_redacao_grade_id,
            :ocr_confianca, :arquivo_anonimo_nome_armazenamento
        )
        """
    )

    CHUNK = 500
    inserted = 0

    try:
        for start in range(0, total, CHUNK):
            end = min(start + CHUNK, total)
            batch = df_insert.iloc[start:end]
            payload: List[Dict] = batch.to_dict(orient="records")

            with engine.begin() as conn:
                conn.execute(insert_sql, payload)

            inserted = end
            pct = int(round(inserted * 100 / total))
            # Atualiza barra de progresso custom
            progress_placeholder.markdown(
                f"<div class='progress-wrap'><div class='progress-bar' style='width:{pct}%'></div></div><div class='progress-label'>{pct}%</div>",
                unsafe_allow_html=True,
            )
        st.success(f"Importação concluída: {inserted} registro(s) inserido(s). ✅")
    except SQLAlchemyError as e:
        st.error("Falha ao inserir registros. Verifique se as colunas e tipos estão corretos.")
        st.stop()

st.markdown('</div>', unsafe_allow_html=True)

# ===================== Rodapé =====================
st.caption("© CorreigeAI • Importador de Redações • Segurança de credenciais por .env")
