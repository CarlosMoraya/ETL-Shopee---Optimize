"""
Configurações e utilitários do projeto
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente
env_path = Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
    print(f".env carregado de {env_path}")
else:
    print(".env não encontrado, usando variáveis de ambiente da nuvem")


# Configurações do projeto
PROJECT_ROOT = Path(__file__).parent.parent.parent

# Em ambientes serverless do Google Cloud (Cloud Run, Cloud Functions),
# as gravações devem acontecer preferencialmente e de forma nativa em /tmp
# O Cloud Run usa variáveis como K_SERVICE, CLOUD_RUN_JOB, etc.
if os.environ.get("K_SERVICE") or os.environ.get("CLOUD_RUN_JOB") or os.environ.get("GOOGLE_CLOUD_PROJECT"):
    BASE_DIR = Path("/tmp")
else:
    BASE_DIR = PROJECT_ROOT

DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Garantir que os diretórios existem
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def get_env(key: str, default: str = None) -> str:
    """
    Obtém uma variável de ambiente.
    
    Args:
        key: Nome da variável
        default: Valor padrão se não existir
        
    Returns:
        str: Valor da variável ou default
    """
    value = os.environ.get(key, default)
    if value is None:
        raise ValueError(f"Variável de ambiente {key} não encontrada")
    return value


def get_neon_connection_string() -> str:
    """
    Obtém a string de conexão do Neon.
    
    Returns:
        str: Connection string formatada
    """
    return get_env("NEON_DATABASE_URL")


# Configurações de credenciais Shopee
SHOPEE_EMAIL = os.environ.get("SHOPEE_EMAIL", "carlos.souto@optimizelog.com.br")
SHOPEE_PWD = os.environ.get("SHOPEE_PWD")
