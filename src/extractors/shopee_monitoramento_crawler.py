"""
Extractor: Visão Geral de Motoristas - Total Expedido
Fonte: Shopee Logistics - Login automático via Playwright + API
Destino: data/raw/shopee_monitoramento/processed_*.csv

Autentica com email/senha reais. Não precisa de cookies manuais.
"""
import asyncio
import requests
import os
from pathlib import Path
from datetime import datetime
import time

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

LOGIN_PAGE_URL = "https://accounts.myagencyservice.com.br"
BASE_URL = "https://logistics.myagencyservice.com.br"
EXPORT_URL = f"{BASE_URL}/mgmt/api/pc/agency/metric/lm/export_fleet_list_v2"


async def fazer_login_e_obter_cookies() -> dict:
    """
    Abre um browser headless, faz login com email/senha
    e retorna os cookies de sessão prontos para uso.
    """
    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception(
            "SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets do GitHub."
        )

    logger.info("Iniciando Playwright para autenticação...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            locale="pt-BR",
        )
        page = await context.new_page()

        try:
            logger.info(f"Abrindo página de login: {LOGIN_PAGE_URL}")
            await page.goto(LOGIN_PAGE_URL, wait_until="networkidle", timeout=60_000)

            logger.info("Preenchendo credenciais...")

            email_field = page.locator(
                "input[type='email'], input[name='email'], input[placeholder*='mail' i], input[id*='email' i]"
            ).first
            await email_field.fill(email)

            senha_field = page.locator("input[type='password']").first
            await senha_field.fill(senha)

            logger.info("Clicando em login...")
            botao_login = page.locator(
                "button[type='submit'], button:has-text('Login'), button:has-text('Entrar'), button:has-text('Sign in')"
            ).first
            await botao_login.click()

            logger.info("Aguardando redirecionamento para o portal...")
            await page.wait_for_url(f"{BASE_URL}/**", timeout=60_000)
            await page.wait_for_load_state("networkidle", timeout=30_000)

            logger.info(f"Redirecionado para: {page.url}")

            raw_cookies = await context.cookies()
            cookies = {c["name"]: c["value"] for c in raw_cookies}

            essenciais = ["fms_user_skey", "spx_uk", "fms_user_id"]
            faltando = [c for c in essenciais if c not in cookies]
            if faltando:
                logger.warning(f"Cookies esperados não encontrados: {faltando}")
                logger.warning(f"Cookies obtidos: {list(cookies.keys())}")
            else:
                logger.info(f"✅ Login bem-sucedido! Cookies obtidos: {list(cookies.keys())}")

            return cookies

        except PlaywrightTimeoutError as e:
            screenshot_path = DATA_RAW_DIR / "login_erro.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(screenshot_path))
            logger.error(f"Timeout durante login. Screenshot salvo em: {screenshot_path}")
            raise Exception(f"Timeout no login Playwright: {e}") from e

        finally:
            await browser.close()


class ShopeeExtractor:
    """
    Extrator da Shopee — usa cookies obtidos via Playwright.
    """

    def __init__(self, cookies: dict):
        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/119.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/",
        })

        self.session.cookies.update(cookies)

    def baixar_export(self, output_path: Path) -> Path:
        """
        Chama a API de exportação e salva o arquivo retornado.
        """
        logger.info("Iniciando download da API...")

        payload = {
            "agency_name": "DELUNA",
            "first_delivering_start": None,
            "first_delivering_end": None,
            "delivered_pct_end": None,
            "delivered_pct_start": None,
            "view_mode": 1,
        }

        self.session.headers.update({"Content-Type": "application/json;charset=UTF-8"})

        logger.info(f"POST {EXPORT_URL}")
        response = self.session.post(EXPORT_URL, json=payload, timeout=60)

        logger.info(f"Status: {response.status_code}")

        if response.status_code == 200:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            content_type = response.headers.get("Content-Type", "")
            content_disp = response.headers.get("Content-Disposition", "")
            logger.info(f"Content-Type: {content_type}")
            logger.info(f"Content-Disposition: {content_disp}")
            logger.info(f"Primeiros bytes: {response.content[:20]}")

            if "csv" in content_type:
                ext = ".csv"
            elif "spreadsheet" in content_type or "excel" in content_type:
                ext = ".xlsx"
            elif content_disp and "filename=" in content_disp:
                nome = content_disp.split("filename=")[1].strip('"\'')
                ext = Path(nome).suffix or ".xlsx"
            else:
                magic = response.content[:4]
                ext = ".xlsx" if magic == b'PK\x03\x04' else ".csv"

            caminho_arquivo = output_path / f"shopee_motoristas_{timestamp}{ext}"
            with open(caminho_arquivo, "wb") as f:
                f.write(response.content)

            logger.info(f"✅ Arquivo baixado: {caminho_arquivo} ({len(response.content)} bytes)")
            return caminho_arquivo

        else:
            logger.error(f"Erro no download: {response.status_code}")
            logger.error(f"Response: {response.text[:500]}")
            raise Exception(f"Falha no download: {response.status_code}")

    def close(self):
        self.session.close()


async def extract_shopee_monitoramento() -> Path:
    """
    Extrai dados de monitoramento de motoristas da Shopee.
    Faz login automático via Playwright, sem necessidade de cookies manuais.
    """
    import pandas as pd

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Monitoramento de Motoristas")
    logger.info("=" * 80)

    output_path = DATA_RAW_DIR / "shopee_monitoramento"
    output_path.mkdir(parents=True, exist_ok=True)

    cookies = await fazer_login_e_obter_cookies()

    extractor = ShopeeExtractor(cookies)

    try:
        time.sleep(1)

        arquivo_baixado = extractor.baixar_export(output_path)

        sufixo = Path(arquivo_baixado).suffix.lower()
        logger.info(f"Lendo arquivo ({sufixo}): {arquivo_baixado}")
        if sufixo == ".csv":
            df = pd.read_csv(arquivo_baixado)
        else:
            df = pd.read_excel(arquivo_baixado, engine="openpyxl")

        logger.info("Transformando dados...")
        if "Driver Name" in df.columns:
            extracao = df["Driver Name"].str.extract(r"\[(.*?)\]\s*(.*)")
            df.insert(0, "driver_id", extracao[0])
            df["Driver Name"] = extracao[1]
            df["driver_id"] = df["driver_id"].fillna("")
            df["Driver Name"] = df["Driver Name"].fillna("")

        df.columns = (
            df.columns
            .str.replace("（", "(").str.replace("）", ")")
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("(#)", "_qtd", regex=False)
            .str.replace("(%)", "_perc", regex=False)
            .str.replace("(", "").str.replace(")", "")
            .str.replace("-", "_")
            .str.replace("__", "_")
            .str.strip("_")
        )

        if "expected_delivered_percentage_perc" in df.columns:
            df = df.rename(columns={"expected_delivered_percentage_perc": "expected_delivered_percentage"})

        df["extracted_at"] = datetime.now()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_file = output_path / f"processed_{timestamp}.csv"
        df.to_csv(processed_file, index=False)
        logger.info(f"Dados processados salvos em: {processed_file}")

        logger.info("\n=== TOTAIS EXTRAÍDOS ===")
        if "assigned" in df.columns:
            logger.info(f"Total Assigned: {df['assigned'].sum()}")
        if "handed_over" in df.columns:
            logger.info(f"Total Handed Over: {df['handed_over'].sum()}")
        if "delivered_qtd" in df.columns:
            logger.info(f"Total Delivered: {df['delivered_qtd'].sum()}")
        logger.info(f"Total Motoristas: {len(df)}")
        logger.info("========================\n")

        return processed_file

    finally:
        extractor.close()


if __name__ == "__main__":
    async def run():
        try:
            arquivo_processado = await extract_shopee_monitoramento()
            logger.info(f"✅ Extração concluída: {arquivo_processado}")
        except Exception as e:
            logger.error(f"❌ Falha na extração: {e}")
            raise

    asyncio.run(run())
