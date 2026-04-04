"""
Extractor: Atribuição de Entrega
Fonte: Shopee Logistics - Login via Playwright (navegação real no portal)
Destino: data/raw/shopee_atribuicao/processed_*.csv

Fluxo:
1. Login no portal
2. Expandir menu "Entrega LM" → clicar "Atribuição de Entrega"
3. Flegar todas as Estações (checkbox select-all)
4. Clicar "Exportar AT"
5. Aguardar 90s e abrir painel "Última tarefa"
6. Clicar "Baixar"
7. Tratar com pandas
"""
import asyncio
import os
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"
ATRIBUICAO_URL = "https://logistics.myagencyservice.com.br/#/agency-assignment/list"


async def extract_shopee_atribuicao() -> Path:
    import pandas as pd

    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets.")

    output_path = DATA_RAW_DIR / "shopee_atribuicao"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Atribuição de Entrega")
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
            accept_downloads=True,
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
            login_ok = False
            for seletor_menu in [
                'text="Força de trabalho"',
                'text="Dashboard"',
                'text="Home"',
                '.nav-menu',
                '.sidebar',
                '[class*="menu"]',
                '[class*="sidebar"]',
            ]:
                try:
                    await page.locator(seletor_menu).first.wait_for(timeout=60_000)
                    logger.info(f"✅ Login confirmado — elemento '{seletor_menu}' carregado!")
                    login_ok = True
                    break
                except Exception:
                    pass
            if not login_ok:
                await page.screenshot(path=str(output_path / "login_erro.png"))
                raise Exception("Login falhou — credenciais incorretas ou portal travou.")

            # 2. NAVEGAR PARA ATRIBUIÇÃO DE ENTREGA
            # Navega diretamente pela URL (evita necessidade de expandir menu manualmente)
            logger.info(f"Navegando para: {ATRIBUICAO_URL}")
            await page.goto(ATRIBUICAO_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(10_000)
            await page.screenshot(path=str(output_path / "pagina_carregada.png"))
            logger.info("✅ Página de Atribuição de Entrega carregada.")

            # 3. SELECIONAR TODAS AS PÁGINAS via dropdown de seleção
            logger.info("Clicando no dropdown de seleção (seta SVG)...")
            try:
                seta_dropdown = page.locator('svg[viewBox="0 0 17 16"]').first
                await seta_dropdown.wait_for(timeout=20_000)
                await seta_dropdown.click()
                await page.wait_for_timeout(1_500)
                logger.info("Clicando em 'Select All in All Pages'...")
                await page.wait_for_timeout(1_000)
                opcao_all_pages = page.locator('.ssc-react-table-selection-menu-item', has_text="Select All in All Pages")
                await opcao_all_pages.click(force=True)
                await page.wait_for_timeout(2_000)
                logger.info("✅ 'Select All in All Pages' selecionado.")
                await page.wait_for_timeout(20_000)
            except Exception as e:
                logger.warning(f"Dropdown de seleção não encontrado: {e}")
                await page.screenshot(path=str(output_path / "erro_select_all_pages.png"))

            # 4. CLICAR EM "EXPORTAR AT" — abre o painel "Última tarefa" e dispara o export
            logger.info("Clicando em 'Exportar AT'...")
            botao_exportar = page.locator('button:has-text("Exportar AT")').first
            await botao_exportar.wait_for(timeout=10_000)
            await botao_exportar.click()
            await page.wait_for_timeout(2_000)

            # Tratar modal de confirmação, se aparecer
            for seletor_confirmar in [
                'button:has-text("Confirm")',
                'button:has-text("Confirmar")',
                'button:has-text("OK")',
                'button:has-text("Yes")',
                'button:has-text("Sim")',
                '.ant-btn-primary:has-text("OK")',
                '.ant-btn-primary:has-text("Confirm")',
            ]:
                try:
                    btn = page.locator(seletor_confirmar).first
                    if await btn.is_visible():
                        logger.info(f"Modal de confirmação detectado — clicando '{seletor_confirmar}'")
                        await btn.click()
                        await page.wait_for_timeout(1_500)
                        break
                except Exception:
                    pass

            await page.screenshot(path=str(output_path / "pos_exportar_at.png"))
            logger.info("Exportação solicitada — aguardando 30s antes de abrir o painel...")
            await page.wait_for_timeout(30_000)

            caminho_arquivo = None
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 6. ABRIR PAINEL → VER TUDO → CLICAR BAIXAR NO PRIMEIRO REGISTRO
            logger.info("Abrindo painel 'Última tarefa' via ícone...")
            icone = page.locator('div[data-v-13320df0].icon').first
            await icone.wait_for(timeout=10_000)
            await icone.click()
            await page.wait_for_timeout(2_000)

            logger.info("Clicando em 'Ver tudo'...")
            ver_tudo = page.locator('button:has-text("Ver tudo")').first
            await ver_tudo.wait_for(timeout=10_000)
            await ver_tudo.click()
            await page.wait_for_timeout(3_000)
            await page.screenshot(path=str(output_path / "export_task_center.png"))
            logger.info("✅ Página de histórico de exports carregada.")

            logger.info("Clicando em 'Baixar' no primeiro registro...")
            XPATH_BAIXAR = (
                '//*[@id="agency-container"]/div[2]/div[2]/div[2]/div/div/div[1]'
                '/div[8]/div/div[1]/div/div[2]/div[1]/div[1]/div[2]/div/div/div'
                '/table/tbody[2]/tr[1]/td[7]/div/div/button'
            )
            botao_baixar = page.locator(f'xpath={XPATH_BAIXAR}').first
            await botao_baixar.wait_for(timeout=15_000)

            async with page.expect_download(timeout=300_000) as download_info:
                await botao_baixar.click()
            download = await download_info.value

            ext = Path(download.suggested_filename).suffix or ".zip"
            caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
            await download.save_as(caminho_arquivo)
            logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")

        finally:
            await browser.close()

    # 8. PROCESSAR COM PANDAS
    logger.info("Processando arquivo...")
    sufixo = Path(caminho_arquivo).suffix.lower()
    if sufixo == ".zip":
        import zipfile
        logger.info("Arquivo ZIP detectado — descompactando...")
        with zipfile.ZipFile(caminho_arquivo, "r") as z:
            nomes = z.namelist()
            logger.info(f"Conteúdo do ZIP: {nomes}")
            z.extractall(output_path)
        caminho_interno = output_path / nomes[0]
        ext_interna = Path(nomes[0]).suffix.lower()
        df = pd.read_csv(caminho_interno) if ext_interna == ".csv" else pd.read_excel(caminho_interno)
    elif sufixo == ".csv":
        df = pd.read_csv(caminho_arquivo)
    else:
        df = pd.read_excel(caminho_arquivo)

    logger.info(f"Linhas brutas: {len(df)} | Colunas: {len(df.columns)}")

    # Extrair driver_id entre colchetes se houver coluna de motorista
    col_motorista = next((c for c in df.columns if "motorista" in c.lower() or "driver" in c.lower()), None)
    if col_motorista:
        logger.info(f"Extraindo driver_id da coluna '{col_motorista}'...")
        extracao = df[col_motorista].astype(str).str.extract(r"\[(.*?)\]\s*(.*)")
        df.insert(0, "driver_id", extracao[0].fillna(""))
        df[col_motorista] = extracao[1].fillna(df[col_motorista])

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
    logger.info(f"Total Atribuições: {len(df)}")

    processed_file = output_path / f"processed_{timestamp}.csv"
    df.to_csv(processed_file, index=False)
    logger.info(f"Dados processados salvos: {processed_file}")

    return processed_file


async def run():
    try:
        arquivo = await extract_shopee_atribuicao()
        logger.info(f"✅ Extração concluída: {arquivo}")
        return str(arquivo)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
