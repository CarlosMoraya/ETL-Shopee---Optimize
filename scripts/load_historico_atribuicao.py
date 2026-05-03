"""
Carga histórica única: shopee_atribuicao + shopee_atribuicao_uniq_at
Usa psycopg2 COPY para carregar 200k+ linhas no Neon sem limites de parâmetros.
"""
import sys
import io
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine, text, Text

from src.utils import get_logger, get_supabase_connection_string

logger = get_logger(__name__)

ARQUIVOS = [
    Path("arquivos/br_agency_assignment_task_20260429.csv"),
    Path("arquivos/br_agency_assignment_task_20260429_1.csv"),
]

TABLE_COMPLETO = "shopee_atribuicao"
TABLE_UNIQ     = "shopee_atribuicao_uniq_at"
SCHEMA         = "public"


def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    col_motorista = next(
        (c for c in df.columns if "motorista" in c.lower() or "driver" in c.lower()), None
    )
    if col_motorista:
        extracao = df[col_motorista].astype(str).str.extract(r"\[(.*?)\]\s*(.*)")
        df.insert(0, "driver_id", extracao[0].fillna(""))
        df[col_motorista] = extracao[1].fillna(df[col_motorista])

    df.columns = (
        df.columns
        .str.replace("（", "(").str.replace("）", ")")
        .str.replace("'", "").str.replace('"', "")
        .str.strip().str.lower().str.replace(" ", "_")
        .str.replace("(#)", "_qtd", regex=False)
        .str.replace("(%)", "_perc", regex=False)
        .str.replace("(", "").str.replace(")", "")
        .str.replace("-", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
        .str.replace("__", "_").str.strip("_")
    )

    df["extracted_at"] = datetime.now()
    df.insert(0, "embarcador", "Shopee_Last_Mile")
    return df


def copy_load(df: pd.DataFrame, table_name: str):
    """Cria/recria a tabela via to_sql (1 linha) e depois COPY para o bulk."""
    conn_str = get_supabase_connection_string()
    if "?sslmode=require" not in conn_str:
        conn_str += "?sslmode=require"

    engine = create_engine(conn_str, pool_pre_ping=True)

    # Passo 1: truncar tabela existente e corrigir colunas com tipo errado
    logger.info(f"Preparando tabela '{table_name}'...")
    with engine.connect() as conn:
        exists = conn.execute(text(
            f"SELECT EXISTS (SELECT FROM information_schema.tables "
            f"WHERE table_schema='{SCHEMA}' AND table_name='{table_name}')"
        )).scalar()

        if exists:
            # Corrigir colunas que deveriam ser TEXT mas foram criadas como numeric
            wrong_cols = conn.execute(text(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = '{SCHEMA}' AND table_name = '{table_name}'
                  AND data_type IN ('double precision','real','numeric','integer','bigint')
                  AND column_name IN ({', '.join([f"'{c}'" for c in df.select_dtypes(include=['object']).columns])})
            """)).fetchall()
            for (col,) in wrong_cols:
                logger.info(f"  Corrigindo tipo da coluna '{col}' para TEXT...")
                conn.execute(text(
                    f"ALTER TABLE {SCHEMA}.{table_name} "
                    f"ALTER COLUMN {col} TYPE TEXT USING {col}::TEXT"
                ))
            conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table_name}"))
            conn.commit()
            logger.info(f"Tabela truncada{' e colunas corrigidas' if wrong_cols else ''}.")
        else:
            # Tabela não existe: criar com schema correto
            dtype_map = {col: Text() for col in df.select_dtypes(include=["object"]).columns}
            df.head(1).to_sql(table_name, engine, schema=SCHEMA, if_exists="replace",
                              index=False, method=None, dtype=dtype_map)
            with engine.connect() as conn2:
                conn2.execute(text(f"TRUNCATE TABLE {SCHEMA}.{table_name}"))
                conn2.commit()
            logger.info(f"Tabela criada com schema correto.")

    engine.dispose()

    # Passo 2: COPY bulk via psycopg2
    logger.info(f"Iniciando COPY de {len(df)} linhas para '{table_name}'...")
    conn_pg = psycopg2.connect(conn_str)
    try:
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep="")
        buf.seek(0)

        cols = ", ".join(df.columns.tolist())
        with conn_pg.cursor() as cur:
            cur.copy_expert(
                f"COPY {SCHEMA}.{table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                buf,
            )
        conn_pg.commit()
        logger.info(f"✅ COPY concluído: {len(df)} linhas em '{table_name}'")
        return len(df)
    finally:
        conn_pg.close()


def main():
    logger.info("=" * 70)
    logger.info("CARGA HISTÓRICA: Shopee Atribuição de Entrega")
    logger.info("=" * 70)

    frames = []
    for path in ARQUIVOS:
        logger.info(f"Lendo {path}...")
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        logger.info(f"  {len(df)} linhas, {len(df.columns)} colunas")
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"Total concatenado: {len(df)} linhas")

    df = normalizar(df)
    logger.info(f"Colunas: {list(df.columns)}")

    # Carga completa
    copy_load(df, TABLE_COMPLETO)

    # Tabela de ATs únicas
    possiveis_at = ["assignment_task_id", "at", "id_da_at", "task_id"]
    col_at = next((c for c in possiveis_at if c in df.columns), None)
    if not col_at:
        col_at = next((c for c in df.columns if "task_id" in c or "at_" in c or "_at" in c), None)

    if col_at:
        df_uniq = df.drop_duplicates(subset=[col_at]).copy()
        logger.info(f"ATs únicas: {len(df_uniq)} (coluna '{col_at}')")
    else:
        logger.warning("Coluna de AT não identificada — usando todas as linhas para uniq.")
        df_uniq = df.copy()

    colunas_pedido = [c for c in df_uniq.columns if "pedido" in c]
    if colunas_pedido:
        df_uniq = df_uniq.drop(columns=colunas_pedido)

    copy_load(df_uniq, TABLE_UNIQ)

    logger.info("\n" + "=" * 70)
    logger.info("CARGA HISTÓRICA CONCLUÍDA")
    logger.info(f"  {TABLE_COMPLETO}: {len(df)} linhas")
    logger.info(f"  {TABLE_UNIQ}:     {len(df_uniq)} linhas")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
