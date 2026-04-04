"""
Extractor: Perfil do Motorista
Fonte: Shopee Logistics - Login via Playwright (navegação real no portal)
Destino: data/raw/shopee_driver_profile/processed_*.csv

Fluxo:
1. Login no portal
2. Navegar para Perfil do Motorista
3. Clicar em "Procurar"
4. Clicar em "Exportar" → "Exportar" (abre painel lateral com tarefa assíncrona)
5. Aguardar processamento e clicar em "Baixar"
6. Tratar com pandas
"""
import asyncio
import os
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"
DRIVER_PROFILE_URL = "https://logistics.myagencyservice.com.br/#/workforce/driver-profile/list"


async def extract_shopee_driver_profile() -> Path:
    import pandas as pd

    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets.")

    output_path = DATA_RAW_DIR / "shopee_driver_profile"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Perfil do Motorista")
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
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        try:
            # 1. LOGIN
            logger.info(f"Acessando portal: {PORTAL_URL}")
            await page.goto(PORTAL_URL, wait_until="networkidle", timeout=60_000)

            logger.info("Aguardando formulário de login...")
            await page.locator('input[type="password"]').wait_for(timeout=30_000)

            logger.info("Preenchendo credenciais...")
            await page.locator('input[autocomplete="email"]').fill(email)
            await page.locator('input[type="password"]').fill(senha)

            logger.info("Submetendo login...")
            await page.locator('input[type="password"]').press("Enter")

            logger.info("Aguardando portal carregar após login...")
            try:
                await page.locator('text="Força de trabalho"').wait_for(timeout=30_000)
                logger.info("✅ Login confirmado — menu principal carregado!")
            except Exception:
                screenshot_path = output_path / "login_erro.png"
                await page.screenshot(path=str(screenshot_path))
                raise Exception("Login falhou — credenciais incorretas ou portal travou.")

            # 2. NAVEGAR PARA PERFIL DO MOTORISTA
            logger.info(f"Navegando para: {DRIVER_PROFILE_URL}")
            await page.goto(DRIVER_PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(10_000)

            # 3. ABRIR LOG PRIMEIRO para contar exports existentes ANTES de disparar
            logger.info("Abrindo Log para contar exports anteriores...")
            log_btn = page.locator('button:has-text("Log"), span:has-text("Log"), text=Log').first
            await log_btn.wait_for(timeout=10_000)
            await log_btn.click()
            await page.wait_for_timeout(3_000)

            botoes_baixar = page.locator('button:has-text("Baixar"), button:has-text("Download")')
            count_antes = await botoes_baixar.count()
            logger.info(f"Exports anteriores no Log: {count_antes}")

            # 4. VOLTAR PARA A LISTA E CLICAR EM "PROCURAR"
            logger.info(f"Voltando para lista: {DRIVER_PROFILE_URL}")
            await page.goto(DRIVER_PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(10_000)

            logger.info("Aguardando tabela carregar...")
            await page.wait_for_selector(".ssc-react-pro-table-table", timeout=60_000)
            await page.wait_for_timeout(3_000)

            logger.info("Clicando em 'Procurar'...")
            try:
                botao_procurar = page.locator('button:has-text("Procurar")').first
                await botao_procurar.wait_for(timeout=20_000)
                await botao_procurar.click()
                logger.info("'Procurar' clicado — aguardando dados...")
                await page.wait_for_timeout(10_000)
            except Exception as e:
                logger.warning(f"Botão 'Procurar' não encontrado: {e}")

            # 5. EXPORTAR (abre dropdown)
            logger.info("Clicando em 'Exportar'...")
            try:
                botao_exportar = page.locator('button:has-text("Exportar")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()
            except Exception:
                botao_exportar = page.locator('button:has-text("Export")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()

            await page.wait_for_timeout(2_000)

            # 6. CLICAR NA OPÇÃO "EXPORTAR" DO DROPDOWN (segunda ocorrência)
            logger.info("Clicando na opção 'Exportar' do dropdown...")
            opcao = page.locator('text=Exportar').nth(1)
            await opcao.wait_for(timeout=10_000)
            await opcao.click()
            logger.info("Exportação solicitada — aguardando processamento...")
            await page.wait_for_timeout(5_000)

            # 7. VOLTAR AO LOG E AGUARDAR NOVO "BAIXAR"
            logger.info("Abrindo Log para aguardar novo export...")
            log_btn = page.locator('button:has-text("Log"), span:has-text("Log"), text=Log').first
            await log_btn.wait_for(timeout=10_000)
            await log_btn.click()
            await page.wait_for_timeout(3_000)

            logger.info(f"Aguardando novo export aparecer (count_antes={count_antes}, timeout=300s)...")
            caminho_arquivo = None

            for tentativa in range(300):
                await page.wait_for_timeout(1_000)
                botoes_baixar = page.locator('button:has-text("Baixar"), button:has-text("Download")')
                count = await botoes_baixar.count()
                if count > count_antes:
                    logger.info(f"✅ Novo export disponível após {tentativa + 1}s! Botões: {count}")
                    break
                if (tentativa + 1) % 30 == 0:
                    logger.info(f"Aguardando... ({tentativa + 1}/300s) — botões: {count}")
                    await page.screenshot(path=str(output_path / f"aguardando_{tentativa+1}s.png"))
                    # Recarrega a página do Log para atualizar a lista
                    await page.reload(wait_until="domcontentloaded")
                    await page.wait_for_timeout(3_000)
            else:
                await page.screenshot(path=str(output_path / "erro_timeout_download.png"))
                raise Exception("Timeout: novo export não apareceu em 300s.")

            # 8. DOWNLOAD — clica no PRIMEIRO botão (mais recente, topo da lista)
            logger.info("Clicando em 'Baixar' no export mais recente...")
            async with page.expect_download(timeout=120_000) as download_info:
                await botoes_baixar.first.click()

            download = await download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_arquivo = output_path / f"shopee_driver_profile_{timestamp}_{download.suggested_filename}"
            await download.save_as(str(caminho_arquivo))
            logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")

        finally:
            await browser.close()

    # 8. PROCESSAR COM PANDAS
    logger.info("Processando arquivo...")
    sufixo = Path(caminho_arquivo).suffix.lower()
    if sufixo == ".csv":
        df = pd.read_csv(caminho_arquivo)
    else:
        df = pd.read_excel(caminho_arquivo)

    logger.info(f"Linhas brutas: {len(df)} | Colunas: {len(df.columns)}")

    # Normalizar colunas
    df.columns = (
        df.columns
        .str.replace("（", "(").str.replace("）", ")")
        .str.replace("'", "").str.replace('"', "")
        .str.strip().str.lower().str.replace(" ", "_")
        .str.replace("(#)", "_qtd", regex=False)
        .str.replace("(%)", "_perc", regex=False)
        .str.replace("(", "").str.replace(")", "")
        .str.replace("-", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
        .str.replace("__", "_").str.strip("_")
    )

    df["extracted_at"] = datetime.now()

    logger.info(f"Colunas normalizadas: {list(df.columns)}")
    logger.info(f"Total Motoristas: {len(df)}")

    processed_file = output_path / f"processed_{timestamp}.csv"
    df.to_csv(processed_file, index=False)
    logger.info(f"Dados processados salvos: {processed_file}")

    return processed_file


async def run():
    try:
        arquivo = await extract_shopee_driver_profile()
        logger.info(f"✅ Extração concluída: {arquivo}")
        return str(arquivo)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
