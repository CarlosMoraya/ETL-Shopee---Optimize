"""
Extractor: Visão Geral de Motoristas - Total Expedido
Fonte: Shopee Logistics - API Direta (via Cookies)
Destino: data/raw/shopee_monitoramento_raw.csv

ESTE CRAWLER USA COOKIES - Sem login, sem browser
"""
import requests
import os
from pathlib import Path
from datetime import datetime
import time

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)


class ShopeeExtractor:
    """
    Extrator da Shopee usando cookies de sessão
    """

    BASE_URL = "https://logistics.myagencyservice.com.br"
    EXPORT_URL = f"{BASE_URL}/mgmt/api/pc/agency/metric/lm/export_fleet_list_v2"

    def __init__(self):
        self.session = requests.Session()

        # Listar TODAS as variáveis de ambiente SHOPEE_
        logger.info("=== DEBUG: Variáveis de Ambiente ===")
        shopee_vars = {k: v[:20] + "..." if len(v) > 20 else v 
                       for k, v in os.environ.items() 
                       if k.startswith("SHOPEE_")}
        logger.info(f"Secrets encontrados: {list(shopee_vars.keys())}")
        for k, v in shopee_vars.items():
            logger.info(f"  {k} = {v}")
        logger.info("=====================================\n")

        # Cookies de autenticação
        self.cookies = {
            "fms_display_name": os.environ.get("SHOPEE_FMS_DISPLAY_NAME", ""),
            "fms_user_agency_id": os.environ.get("SHOPEE_FMS_USER_AGENCY_ID", "50"),
            "fms_user_id": os.environ.get("SHOPEE_FMS_USER_ID", ""),
            "fms_user_skey": os.environ.get("SHOPEE_FMS_USER_SKEY", ""),
            "spx_agid": os.environ.get("SHOPEE_SPX_AGID", "50"),
            "spx_cid": os.environ.get("SHOPEE_SPX_CID", "BR"),
            "spx_dn": os.environ.get("SHOPEE_SPX_DN", ""),
            "spx_st": os.environ.get("SHOPEE_SPX_ST", "4"),
            "spx_uid": os.environ.get("SHOPEE_SPX_UID", ""),
            "spx_uk": os.environ.get("SHOPEE_SPX_UK", ""),
            "spx-admin-device-id": os.environ.get("SHOPEE_ADMIN_DEVICE_ID", ""),
            "spx-admin-lang": os.environ.get("SHOPEE_ADMIN_LANG", "pt-br"),
            "ssc_user_role": os.environ.get("SHOPEE_SSC_USER_ROLE", ""),
        }

        # Filtrar cookies vazios
        self.cookies = {k: v for k, v in self.cookies.items() if v}

        # Headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/",
        })

        # Adicionar cookies
        if self.cookies:
            self.session.cookies.update(self.cookies)
            logger.info(f"✅ Cookies configurados ({len(self.cookies)}): {list(self.cookies.keys())}")
            
            # Log dos 3 principais
            if 'fms_user_skey' in self.cookies:
                logger.info(f"  - fms_user_skey: {self.cookies['fms_user_skey'][:20]}...")
            if 'spx_uk' in self.cookies:
                logger.info(f"  - spx_uk: {self.cookies['spx_uk'][:20]}...")
            if 'fms_user_id' in self.cookies:
                logger.info(f"  - fms_user_id: {self.cookies['fms_user_id']}")
        else:
            logger.error("❌ NENHUM cookie configurado! Verifique secrets no GitHub.")

    def testar_conexao(self) -> bool:
        """
        Testa se os cookies são válidos fazendo uma requisição simples.
        """
        logger.info("Testando conexão com cookies...")
        
        try:
            test_url = self.EXPORT_URL
            logger.info(f"URL: {test_url}")
            
            response = self.session.get(test_url, timeout=30)
            
            logger.info(f"Status: {response.status_code}")
            
            # 200 = OK, 404 = URL pode não existir mas cookies válidos
            if response.status_code in [200, 204]:
                logger.info("✅ Cookies válidos! Conexão OK.")
                return True
            elif response.status_code == 404:
                logger.info("⚠️ URL não encontrada (404), mas cookies podem estar OK")
                return True
            elif response.status_code in [401, 403]:
                logger.error(f"❌ Cookies inválidos/expirados (Status: {response.status_code})")
                return False
            else:
                logger.warning(f"Status inesperado: {response.status_code}")
                return True
                
        except Exception as e:
            logger.error(f"Erro no teste: {e}")
            return False

    def baixar_export(self, output_path: Path) -> Path:
        """
        Baixa o arquivo de exportação da API.
        """
        logger.info("Iniciando download da API...")
        
        try:
            params = {
                "type": "total_expedido",
                "timestamp": int(time.time() * 1000),
            }

            logger.info(f"URL: {self.EXPORT_URL}")
            logger.info(f"Params: {params}")

            response = self.session.get(
                self.EXPORT_URL,
                params=params,
                timeout=60
            )

            logger.info(f"Status do download: {response.status_code}")

            if response.status_code == 200:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                content_disp = response.headers.get("Content-Disposition", "")
                if "filename=" in content_disp:
                    filename = content_disp.split("filename=")[1].strip('"\'')
                else:
                    filename = f"shopee_motoristas_{timestamp}.xlsx"

                caminho_arquivo = output_path / filename

                with open(caminho_arquivo, "wb") as f:
                    f.write(response.content)

                logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")
                logger.info(f"Tamanho: {len(response.content)} bytes")

                return caminho_arquivo
            else:
                logger.error(f"Erro no download: {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")

                if response.status_code in [401, 403]:
                    logger.error("Cookies expirados! Atualize os secrets no GitHub.")

                raise Exception(f"Falha no download: {response.status_code}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede no download: {e}")
            raise

    def close(self):
        """Fecha a sessão"""
        self.session.close()


def extract_shopee_monitoramento() -> Path:
    """
    Extrai dados de monitoramento de motoristas da Shopee.
    """
    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Monitoramento de Motoristas")
    logger.info("=" * 80)

    output_path = DATA_RAW_DIR / "shopee_monitoramento"
    output_path.mkdir(parents=True, exist_ok=True)

    extractor = ShopeeExtractor()

    try:
        # Testar conexão
        if not extractor.testar_conexao():
            raise Exception("Falha na conexão. Verifique os cookies.")

        time.sleep(1)

        # Baixar arquivo
        arquivo_baixado = extractor.baixar_export(output_path)

        # Ler e transformar dados
        logger.info("Lendo arquivo Excel...")
        import pandas as pd
        df = pd.read_excel(arquivo_baixado)

        # Transformar (separar ID do nome)
        logger.info("Transformando dados...")
        if 'Driver Name' in df.columns:
            extracao = df['Driver Name'].str.extract(r'\[(.*?)\]\s*(.*)')
            df.insert(0, 'driver_id', extracao[0])
            df['Driver Name'] = extracao[1]
            df['driver_id'] = df['driver_id'].fillna('')
            df['Driver Name'] = df['Driver Name'].fillna('')

        # Normalizar nomes das colunas
        df.columns = (
            df.columns
            .str.replace('（', '(').str.replace('）', ')')
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('(#)', '_qtd', regex=False)
            .str.replace('(%)', '_perc', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('-', '_')
            .str.replace('__', '_')
            .str.strip('_')
        )

        # Corrigir nome problemático
        if 'expected_delivered_percentage_perc' in df.columns:
            df = df.rename(columns={'expected_delivered_percentage_perc': 'expected_delivered_percentage'})

        # Adicionar timestamp
        df['extracted_at'] = datetime.now()

        # Salvar processado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_file = output_path / f"processed_{timestamp}.csv"
        df.to_csv(processed_file, index=False)
        logger.info(f"Dados processados salvos em: {processed_file}")

        # Log de totais
        logger.info("\n=== TOTAIS EXTRAÍDOS ===")
        if 'assigned' in df.columns:
            logger.info(f"Total Assigned: {df['assigned'].sum()}")
        if 'handed_over' in df.columns:
            logger.info(f"Total Handed Over: {df['handed_over'].sum()}")
        if 'delivered_qtd' in df.columns:
            logger.info(f"Total Delivered: {df['delivered_qtd'].sum()}")
        logger.info(f"Total Motoristas: {len(df)}")
        logger.info("========================\n")

        return processed_file

    finally:
        extractor.close()


async def run():
    """
    Função principal para executar o crawler.
    """
    try:
        arquivo_processado = extract_shopee_monitoramento()
        logger.info(f"✅ Extração concluída: {arquivo_processado}")
        return str(arquivo_processado)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
