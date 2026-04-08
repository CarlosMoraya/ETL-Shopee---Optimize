"""
Extractor: Visão Geral de Motoristas - Total Expedido
Fonte: Shopee Logistics - Login via Playwright (navegação real no portal)
Destino: data/raw/shopee_monitoramento/processed_*.csv
"""
import asyncio
import os
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"
DASHBOARD_URL = "https://logistics.myagencyservice.com.br/#/mgmtAgency/lm-hub"


async def extract_shopee_monitoramento() -> Path:
    import pandas as pd

    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets.")

    output_path = DATA_RAW_DIR / "shopee_monitoramento"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Monitoramento de Motoristas")
    logger.info("=" * 80)

    async with async_playwright() as p:
        logger.info("Iniciando navegador...")
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
            # 1. ACESSAR O PORTAL (redireciona para login via OAuth)
            logger.info(f"Acessando portal: {PORTAL_URL}")
            await page.goto(PORTAL_URL, wait_until="networkidle", timeout=60_000)

            # 2. LOGIN
            logger.info("Aguardando formulário de login...")
            await page.locator('input[type="password"]').wait_for(timeout=30_000)

            logger.info("Preenchendo credenciais...")
            await page.locator('input[autocomplete="email"]').fill(email)
            await page.locator('input[type="password"]').fill(senha)

            logger.info("Submetendo login...")
            await page.locator('input[type="password"]').press("Enter")

            # 3. AGUARDAR PORTAL CARREGAR
            logger.info("Aguardando portal carregar após login...")
            try:
                await page.locator('text="Força de trabalho"').wait_for(timeout=30_000)
                logger.info("✅ Login confirmado — menu principal carregado!")
            except Exception:
                screenshot_path = output_path / "login_erro.png"
                await page.screenshot(path=str(screenshot_path))
                logger.error(f"Menu não apareceu após login. Screenshot: {screenshot_path}")
                raise Exception("Login falhou — credenciais incorretas ou portal travou.")

            # 4. NAVEGAR PARA O DASHBOARD
            logger.info(f"Navegando para dashboard: {DASHBOARD_URL}")
            await page.goto(DASHBOARD_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(5_000)

            # 5. CLICAR NA ABA CORRETA
            logger.info("Clicando na aba 'Visão geral dos motoristas'...")
            try:
                aba = page.locator('span:has-text("Visão geral dos motoristas")')
                await aba.wait_for(timeout=20_000)
                await aba.click()
                logger.info("Aba clicada!")
            except Exception as e:
                logger.warning(f"Aba não encontrada ou já ativa: {e}")
                await page.screenshot(path=str(output_path / "erro_dashboard.png"))

            await page.wait_for_timeout(5_000)

            # 6. EXPORTAR
            logger.info("Procurando botão Exportar...")
            export_button = page.locator('button:has-text("Exportar")')
            await export_button.wait_for(timeout=10_000)

            async with page.expect_download(timeout=30_000) as download_info:
                await export_button.click()

            download = await download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_arquivo = output_path / f"shopee_motoristas_{timestamp}_{download.suggested_filename}"
            await download.save_as(str(caminho_arquivo))
            logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")

        finally:
            await browser.close()

    # 7. PROCESSAR COM PANDAS
    logger.info("Processando arquivo...")
    df = pd.read_excel(caminho_arquivo)

    extracao = df["Driver Name"].str.extract(r"\[(.*?)\]\s*(.*)")
    df.insert(0, "driver_id", extracao[0])
    df["Driver Name"] = extracao[1]
    df["driver_id"] = df["driver_id"].fillna("")
    df["Driver Name"] = df["Driver Name"].fillna("")

    df.columns = (
        df.columns
        .str.replace("（", "(").str.replace("）", ")")
        .str.strip().str.lower().str.replace(" ", "_")
        .str.replace("(#)", "_qtd", regex=False)
        .str.replace("(%)", "_perc", regex=False)
        .str.replace("(", "").str.replace(")", "")
        .str.replace("-", "_")
        .str.replace("__", "_").str.strip("_")
    )

    if "expected_delivered_percentage_perc" in df.columns:
        df = df.rename(columns={"expected_delivered_percentage_perc": "expected_delivered_percentage"})

    df["extracted_at"] = datetime.now()

    # Adicionar a nova coluna 'embarcador' na primeira posição
    df.insert(0, "embarcador", "Shopee_Last_Mile")

    logger.info("\n=== TOTAIS EXTRAÍDOS ===")
    for col in ["assigned", "handed_over", "delivered_qtd", "on_hold", "delivering_qtd"]:
        if col in df.columns:
            logger.info(f"Total {col}: {df[col].sum()}")
    logger.info(f"Total Motoristas: {len(df)}")
    logger.info("========================\n")

    processed_file = output_path / f"processed_{timestamp}.csv"
    df.to_csv(processed_file, index=False)
    logger.info(f"Dados processados salvos: {processed_file}")

    return processed_file


async def run():
    try:
        arquivo = await extract_shopee_monitoramento()
        logger.info(f"✅ Extração concluída: {arquivo}")
        return str(arquivo)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
