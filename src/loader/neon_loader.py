"""
Loader para banco de dados Neon (PostgreSQL)
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional
import os

from src.utils import get_logger, get_neon_connection_string

logger = get_logger(__name__)


def create_neon_engine() -> Engine:
    """
    Cria uma engine de conexão com o Neon.
    
    Returns:
        Engine: SQLAlchemy engine configurada
    """
    connection_string = get_neon_connection_string()
    
    # Adicionar pool_size e pool_pre_ping para conexões serverless
    if "?sslmode=require" not in connection_string:
        connection_string += "?sslmode=require"
    
    engine = create_engine(
        connection_string,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    
    logger.info("Engine do Neon criada com sucesso!")
    return engine


def load_to_neon(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    if_exists: str = "append",
    chunksize: int = 1000,
) -> int:
    """
    Carrega um DataFrame no Neon.
    
    Args:
        df: DataFrame com os dados
        table_name: Nome da tabela
        schema: Schema do banco (default: public)
        if_exists: 'append', 'replace' ou 'fail'
        chunksize: Tamanho dos lotes de insert
        
    Returns:
        int: Número de linhas inseridas
    """
    logger.info(f"Iniciando carga para tabela: {schema}.{table_name}")
    logger.info(f"Linhas para inserir: {len(df)}")
    
    engine = create_neon_engine()
    
    try:
        # Criar tabela se não existir
        if if_exists == "append":
            # Verificar se tabela existe
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{schema}' 
                        AND table_name = '{table_name}'
                    );
                """))
                tabela_existe = result.scalar()
                
                if not tabela_existe:
                    logger.info(f"Tabela {schema}.{table_name} não existe. Criando...")
                    if_exists = "replace"
                else:
                    logger.info(f"Tabela {schema}.{table_name} já existe. Append mode.")
        
        # Carregar dados
        rows_inserted = df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        
        logger.info(f"✅ Carga concluída! {rows_inserted} linhas inseridas.")
        return rows_inserted
        
    except Exception as e:
        logger.error(f"❌ Erro na carga: {e}")
        raise
    finally:
        engine.dispose()


def execute_query(query: str, params: dict = None) -> list:
    """
    Executa uma query SQL no Neon.
    
    Args:
        query: Query SQL
        params: Parâmetros da query
        
    Returns:
        list: Resultados da query
    """
    logger.info(f"Executando query: {query[:100]}...")
    
    engine = create_neon_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            
            if result.returns_rows:
                rows = result.fetchall()
                logger.info(f"Query retornou {len(rows)} linhas")
                return rows
            else:
                logger.info("Query executada com sucesso (sem retorno)")
                return []
                
    except Exception as e:
        logger.error(f"❌ Erro na query: {e}")
        raise
    finally:
        engine.dispose()


def upsert_to_neon(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "public",
    conflict_columns: list = None,
) -> int:
    """
    Realiza UPSERT (insert or update) no Neon.
    
    Args:
        df: DataFrame com os dados
        table_name: Nome da tabela
        schema: Schema do banco
        conflict_columns: Colunas para conflito (ON CONFLICT)
        
    Returns:
        int: Número de linhas afetadas
    """
    logger.info(f"Iniciando UPSERT para tabela: {schema}.{table_name}")
    
    if not conflict_columns:
        logger.warning("Nenhuma coluna de conflito especificada. Usando append.")
        return load_to_neon(df, table_name, schema, if_exists="append")
    
    engine = create_neon_engine()
    
    try:
        # Criar tabela temporária
        temp_table = f"{table_name}_temp"

        # Deduplicar pelo(s) conflict_column(s) — mantém última ocorrência
        antes = len(df)
        df = df.drop_duplicates(subset=conflict_columns, keep="last")
        if len(df) < antes:
            logger.warning(f"{antes - len(df)} linha(s) duplicadas removidas do DataFrame antes do upsert.")

        # Inserir dados na temporária
        df.to_sql(
            name=temp_table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi",
        )
        
        logger.info(f"Tabela temporária {temp_table} criada com {len(df)} linhas")
        
        # Construir colunas para INSERT
        columns = df.columns.tolist()
        columns_str = ", ".join(columns)
        values_str = ", ".join([f"EXCLUDED.{col}" for col in columns])
        conflict_cols_str = ", ".join(conflict_columns)
        
        # UPSERT
        query = f"""
            INSERT INTO {schema}.{table_name} ({columns_str})
            SELECT {columns_str} FROM {schema}.{temp_table}
            ON CONFLICT ({conflict_cols_str}) DO UPDATE SET
            {", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()
            rows_affected = result.rowcount
            # Dropar tabela temporária dentro do mesmo bloco de conexão
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            conn.commit()
        
        logger.info(f"✅ UPSERT concluído! {rows_affected} linhas afetadas.")
        return rows_affected
        
    except Exception as e:
        logger.error(f"❌ Erro no UPSERT: {e}")
        raise
    finally:
        engine.dispose()
