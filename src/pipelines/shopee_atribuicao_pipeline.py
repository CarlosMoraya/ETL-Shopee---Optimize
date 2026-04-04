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
        arquivo_processado = await extract_shopee_atribuicao()

        # TRANSFORM
        logger.info("\n🔄 FASE 2: TRANSFORMAÇÃO")
        df = pd.read_csv(arquivo_processado)
        logger.info(f"Linhas carregadas: {len(df)}")
        logger.info(f"Colunas: {list(df.columns)}")

        if len(df) == 0:
            raise Exception("DataFrame vazio — nenhum dado extraído.")

        df["extracted_at"] = datetime.now()

        # LOAD — replace sempre (recria a tabela com os tipos corretos)
        logger.info("\n📤 FASE 3: CARGA")
        logger.info("Carregando com replace...")
        rows_inserted = load_to_neon(
            df=df,
            table_name=table_name,
            schema="public",
            if_exists="replace",
        )

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info(f"   - Linhas extraídas: {len(df)}")
        logger.info(f"   - Linhas afetadas: {rows_inserted}")
        logger.info(f"   - Tabela: {table_name}")
        logger.info(f"   - Modo: replace")
        logger.info("=" * 80)

        return {
            "status": "success",
            "extracted_rows": len(df),
            "inserted_rows": rows_inserted,
            "table": table_name,
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
