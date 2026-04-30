"""
Pipeline ETL: Shopee Atribuição de Entrega
Extract -> Transform -> Load para Neon (tabela: shopee_atribuicao)

Estratégia de carga: replace (recria a tabela a cada execução)
"""
import asyncio
import pandas as pd
from datetime import datetime

from src.utils import get_logger
from src.extractors.shopee_atribuicao_crawler import extract_shopee_atribuicao
from src.loader.neon_loader import load_to_neon, execute_query

logger = get_logger(__name__)

TABLE_NAME = "shopee_atribuicao"
TRACKING_COL = "spx_tracking_num"


def _partial_load(df: pd.DataFrame, table_name: str, schema: str = "public") -> int:
    """Deleta linhas com mesmo spx_tracking_num e reinsere os novos dados."""
    check_rows = execute_query(
        f"SELECT EXISTS (SELECT FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
    )
    tabela_existe = check_rows[0][0] if check_rows else False

    if not tabela_existe:
        logger.info(f"Tabela {table_name} não existe — criando via replace...")
        return load_to_neon(df, table_name, schema, if_exists="replace")

    if TRACKING_COL not in df.columns:
        logger.warning(f"Coluna '{TRACKING_COL}' não encontrada — usando replace completo.")
        return load_to_neon(df, table_name, schema, if_exists="replace")

    tracking_vals = df[TRACKING_COL].dropna().unique().tolist()
    if tracking_vals:
        placeholders = ", ".join([f"'{v}'" for v in tracking_vals])
        execute_query(f"DELETE FROM {schema}.{table_name} WHERE {TRACKING_COL} IN ({placeholders})")
        logger.info(f"Deletadas linhas existentes com {len(tracking_vals)} tracking nums distintos.")

    rows_inserted = load_to_neon(df, table_name, schema, if_exists="append")
    logger.info(f"✅ Carga incremental concluída: {rows_inserted} linhas inseridas.")
    return rows_inserted


async def run_pipeline(table_name: str = TABLE_NAME):
    logger.info("=" * 80)
    logger.info("PIPELINE ETL: SHOPEE ATRIBUIÇÃO DE ENTREGA")
    logger.info("=" * 80)

    try:
        # EXTRACT
        logger.info("\n📥 FASE 1: EXTRAÇÃO")
        arquivos_processados = await extract_shopee_atribuicao()

        # TRANSFORM E CARGA COMPLETO
        logger.info("\n🔄 FASE 2: TRANSFORMAÇÃO E CARGA (COMPLETA)")
        df_completo = pd.read_csv(arquivos_processados["completo"])
        logger.info(f"Linhas carregadas (completo): {len(df_completo)}")

        if len(df_completo) == 0:
            raise Exception("DataFrame vazio — nenhum dado extraído.")

        df_completo["extracted_at"] = datetime.now()

        # LOAD — incremental por spx_tracking_num
        logger.info("\n📤 FASE 3: CARGA (COMPLETA)")
        logger.info("Carregando tabela completa com carga incremental por tracking num...")
        rows_inserted_completo = _partial_load(df_completo, table_name)

        # TRANSFORM E CARGA UNICOS
        logger.info("\n🔄 FASE 4: TRANSFORMAÇÃO E CARGA (ÚNICAS)")
        df_uniq = pd.read_csv(arquivos_processados["uniq"])
        logger.info(f"Linhas carregadas (únicas): {len(df_uniq)}")

        df_uniq["extracted_at"] = datetime.now()

        table_name_uniq = f"{table_name}_uniq_at"
        logger.info("Carregando tabela única com carga incremental por tracking num...")
        rows_inserted_uniq = _partial_load(df_uniq, table_name_uniq)

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info(f"   - Tabela: {table_name} | Linhas inseridas: {rows_inserted_completo}")
        logger.info(f"   - Tabela: {table_name_uniq} | Linhas inseridas: {rows_inserted_uniq}")
        logger.info(f"   - Modo: incremental (delete+insert por {TRACKING_COL})")
        logger.info("=" * 80)

        return {
            "status": "success",
            "inserted_rows_completo": rows_inserted_completo,
            "inserted_rows_uniq": rows_inserted_uniq,
            "tables": [table_name, table_name_uniq],
            "mode": f"incremental ({TRACKING_COL})",
        }

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error(f"❌ PIPELINE FALHOU: {e}")
        logger.error("=" * 80)

        return {
            "status": "error",
            "error": str(e),
        }


async def main():
    resultado = await run_pipeline()
    if resultado["status"] == "error":
        raise Exception(resultado["error"])
    return resultado


if __name__ == "__main__":
    asyncio.run(main())
