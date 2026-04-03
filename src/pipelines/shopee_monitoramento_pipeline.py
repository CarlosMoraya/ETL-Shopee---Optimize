"""
Pipeline ETL: Shopee Monitoramento de Motoristas
Extract -> Transform -> Load para Neon
"""
import asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime

from src.utils import get_logger, DATA_PROCESSED_DIR
from src.extractors.shopee_monitoramento_crawler import extract_shopee_monitoramento
from src.loader.neon_loader import load_to_neon

logger = get_logger(__name__)


def carregar_e_validar(arquivo_csv: Path) -> pd.DataFrame:
    """
    Carrega o CSV processado e valida os dados.
    
    Args:
        arquivo_csv: Caminho do arquivo CSV
        
    Returns:
        pd.DataFrame: DataFrame validado
    """
    logger.info(f"Carregando CSV: {arquivo_csv}")
    
    df = pd.read_csv(arquivo_csv)
    
    logger.info(f"Linhas carregadas: {len(df)}")
    logger.info(f"Colunas: {list(df.columns)}")
    
    # Validações básicas
    if len(df) == 0:
        logger.warning("DataFrame vazio!")
        return df
    
    # Verificar colunas essenciais
    colunas_esperadas = ['driver_id', 'driver_name', 'assigned', 'delivered_qtd']
    colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
    
    if colunas_faltantes:
        logger.warning(f"Colunas faltantes: {colunas_faltantes}")
    
    # Remover duplicatas por driver_id
    if 'driver_id' in df.columns:
        duplicatas = df.duplicated(subset=['driver_id'], keep='first').sum()
        if duplicatas > 0:
            logger.warning(f"Removendo {duplicatas} linhas duplicadas")
            df = df.drop_duplicates(subset=['driver_id'], keep='first')
    
    # Adicionar timestamp da extração
    df['extracted_at'] = datetime.now()
    
    return df


async def run_pipeline(table_name: str = "shopee_monitoramento"):
    """
    Executa o pipeline ETL completo.
    
    Args:
        table_name: Nome da tabela no Neon
    """
    logger.info("=" * 80)
    logger.info("PIPELINE ETL: SHOPEE MONITORAMENTO DE MOTORISTAS")
    logger.info("=" * 80)
    
    try:
        # EXTRACT
        logger.info("\n📥 FASE 1: EXTRAÇÃO")
        arquivo_processado = extract_shopee_monitoramento()
        
        # TRANSFORM
        logger.info("\n🔄 FASE 2: TRANSFORMAÇÃO")
        df = carregar_e_validar(Path(arquivo_processado))
        
        # LOAD
        logger.info("\n📤 FASE 3: CARGA")
        logger.info(f"Tabela destino: {table_name}")
        
        rows_inserted = load_to_neon(
            df=df,
            table_name=table_name,
            schema="public",
            if_exists="replace",
        )
        
        # Resumo
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
    """
    Função principal para rodar o pipeline.
    """
    resultado = await run_pipeline()
    
    if resultado["status"] == "error":
        raise Exception(resultado["error"])
    
    return resultado


if __name__ == "__main__":
    asyncio.run(main())
