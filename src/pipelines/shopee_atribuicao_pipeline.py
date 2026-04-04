"""
Pipeline ETL: Shopee Atribuição de Entrega
Extract -> Transform -> Load para Neon (tabela: shopee_atribuicao)

Estratégia de carga:
- Primeira execução: replace (carga completa)
- Execuções seguintes: upsert por id_da_at (mantém histórico)
"""
import asyncio
import pandas as pd
from datetime import datetime

from src.utils import get_logger
from src.extractors.shopee_atribuicao_crawler import extract_shopee_atribuicao
from src.loader.neon_loader import load_to_neon, upsert_to_neon
from sqlalchemy import create_engine, text
from src.loader.neon_loader import create_neon_engine

logger = get_logger(__name__)

TABLE_NAME = "shopee_atribuicao"
CONFLICT_COLUMNS = ["assignment_task_id"]


def tabela_existe(table_name: str, schema: str = "public") -> bool:
    engine = create_neon_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}'
                );
            """))
            return result.scalar()
    finally:
        engine.dispose()


def garantir_unique_constraint(table_name: str, column: str, schema: str = "public"):
    constraint_name = f"{table_name}_{column}_key"
    engine = create_neon_engine()
    try:
        with engine.connect() as conn:
            existe = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM pg_constraint
                    WHERE conname = '{constraint_name}'
                );
            """)).scalar()
            if not existe:
                conn.execute(text(f"""
                    ALTER TABLE {schema}.{table_name}
                    ADD CONSTRAINT {constraint_name} UNIQUE ({column});
                """))
                conn.commit()
                logger.info(f"UNIQUE constraint criada em {table_name}.{column}")
            else:
                logger.info(f"UNIQUE constraint já existe em {table_name}.{column}")
    finally:
        engine.dispose()


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

        # LOAD — primeira vez: replace; demais: upsert
        logger.info("\n📤 FASE 3: CARGA")
        primeira_carga = not tabela_existe(table_name)

        if primeira_carga:
            logger.info("Primeira carga — criando tabela com replace...")
            rows_inserted = load_to_neon(
                df=df,
                table_name=table_name,
                schema="public",
                if_exists="replace",
            )
            logger.info("Tabela criada. Próximas execuções usarão upsert.")
        else:
            # Verifica se a coluna de conflito existe no DataFrame
            cols_conflito = [c for c in CONFLICT_COLUMNS if c in df.columns]
            if not cols_conflito:
                logger.warning(f"Coluna(s) {CONFLICT_COLUMNS} não encontrada(s) — usando append.")
                rows_inserted = load_to_neon(df=df, table_name=table_name, schema="public", if_exists="append")
            else:
                logger.info(f"Upsert por: {cols_conflito}")
                rows_inserted = upsert_to_neon(
                    df=df,
                    table_name=table_name,
                    schema="public",
                    conflict_columns=cols_conflito,
                )

        garantir_unique_constraint(table_name, CONFLICT_COLUMNS[0])

        logger.info("\n" + "=" * 80)
        logger.info("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info(f"   - Linhas extraídas: {len(df)}")
        logger.info(f"   - Linhas afetadas: {rows_inserted}")
        logger.info(f"   - Tabela: {table_name}")
        logger.info(f"   - Modo: {'replace (1ª carga)' if primeira_carga else 'upsert'}")
        logger.info("=" * 80)

        return {
            "status": "success",
            "extracted_rows": len(df),
            "inserted_rows": rows_inserted,
            "table": table_name,
            "mode": "replace" if primeira_carga else "upsert",
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
