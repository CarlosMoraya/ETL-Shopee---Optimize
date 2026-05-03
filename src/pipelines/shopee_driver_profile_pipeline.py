"""
Pipeline ETL: Shopee Perfil do Motorista
Extract -> Transform -> Load para Supabase (tabela: shopee_base_drivers)
"""
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime

from src.utils import get_logger
from src.extractors.shopee_driver_profile_crawler import extract_shopee_driver_profile
from src.loader.supabase_loader import load_to_supabase

logger = get_logger(__name__)


async def run_pipeline(table_name: str = "shopee_base_drivers"):
    logger.info("=" * 80)
    logger.info("PIPELINE ETL: SHOPEE PERFIL DO MOTORISTA")
    logger.info("=" * 80)

    try:
        # EXTRACT
        logger.info("\n📥 FASE 1: EXTRAÇÃO")
        arquivo_processado = await extract_shopee_driver_profile()

        # TRANSFORM
        logger.info("\n🔄 FASE 2: TRANSFORMAÇÃO")
        df = pd.read_csv(arquivo_processado)
        logger.info(f"Linhas carregadas: {len(df)}")
        logger.info(f"Colunas: {list(df.columns)}")

        if len(df) == 0:
            raise Exception("DataFrame vazio — nenhum dado extraído.")

        df["extracted_at"] = datetime.now()

        # LOAD
        logger.info("\n📤 FASE 3: CARGA")
        logger.info(f"Tabela destino: {table_name}")

        rows_inserted = load_to_supabase(
            df=df,
            table_name=table_name,
            schema="public",
            if_exists="replace",
        )

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info(f"   - Linhas extraídas: {len(df)}")
        logger.info(f"   - Linhas inseridas: {rows_inserted}")
        logger.info(f"   - Tabela: {table_name}")
        logger.info("=" * 80)

        return {
            "status": "success",
            "extracted_rows": len(df),
            "inserted_rows": rows_inserted,
            "table": table_name,
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
