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
from src.loader.neon_loader import load_to_neon

logger = get_logger(__name__)

TABLE_NAME = "shopee_atribuicao"


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

        # LOAD — replace sempre (recria a tabela com os tipos corretos)
        logger.info("\n📤 FASE 3: CARGA (COMPLETA)")
        logger.info("Carregando tabela completa com replace...")
        rows_inserted_completo = load_to_neon(
            df=df_completo,
            table_name=table_name,
            schema="public",
            if_exists="replace",
        )

        # TRANSFORM E CARGA UNICOS
        logger.info("\n🔄 FASE 4: TRANSFORMAÇÃO E CARGA (ÚNICAS)")
        df_uniq = pd.read_csv(arquivos_processados["uniq"])
        logger.info(f"Linhas carregadas (únicas): {len(df_uniq)}")

        df_uniq["extracted_at"] = datetime.now()

        table_name_uniq = f"{table_name}_uniq_at"
        logger.info("Carregando tabela única com replace...")
        rows_inserted_uniq = load_to_neon(
            df=df_uniq,
            table_name=table_name_uniq,
            schema="public",
            if_exists="replace",
        )

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info(f"   - Tabela: {table_name} | Linhas inseridas: {rows_inserted_completo}")
        logger.info(f"   - Tabela: {table_name_uniq} | Linhas inseridas: {rows_inserted_uniq}")
        logger.info(f"   - Modo: replace")
        logger.info("=" * 80)

        return {
            "status": "success",
            "inserted_rows_completo": rows_inserted_completo,
            "inserted_rows_uniq": rows_inserted_uniq,
            "tables": [table_name, table_name_uniq],
            "mode": "replace",
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
