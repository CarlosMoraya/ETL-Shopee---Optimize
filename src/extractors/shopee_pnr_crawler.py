"""
Extractor: Ticket PNR (Todos os Tickets)
Fonte: Shopee Logistics - Login via Playwright (navegação real no portal)
Destino: data/raw/shopee_pnr/processed_*.csv

Fluxo:
1. Login no portal
2. Navegar para Ticket PNR → aba "Todos os Tickets"
3. Clicar em "Exportar"
4. Aguardar processamento (120s) e abrir painel "Última tarefa"
5. Clicar em "Baixar"
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
PNR_URL = "https://logistics.myagencyservice.com.br/#/agency-pnr-ticket/list"


async def extract_shopee_pnr() -> Path:
    import pandas as pd

    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets.")

    output_path = DATA_RAW_DIR / "shopee_pnr"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Ticket PNR")
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
                await page.screenshot(path=str(output_path / "login_erro.png"))
                raise Exception("Login falhou — credenciais incorretas ou portal travou.")

            # 2. NAVEGAR PARA TICKET PNR
            logger.info(f"Navegando para: {PNR_URL}")
            await page.goto(PNR_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(10_000)

            # 3. CLICAR NA ABA "TODOS OS TICKETS"
            logger.info("Clicando na aba 'Todos os Tickets'...")
            try:
                aba = page.locator('text="Todos os Tickets"').first
                await aba.wait_for(timeout=15_000)
                await aba.click()
                await page.wait_for_timeout(5_000)
                logger.info("✅ Aba 'Todos os Tickets' selecionada.")
            except Exception as e:
                logger.warning(f"Aba 'Todos os Tickets' não encontrada, continuando: {e}")

            # 4. EXPORTAR
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

            # Verifica se abriu dropdown (segunda ocorrência de "Exportar")
            opcoes = page.locator('text=Exportar')
            count_opcoes = await opcoes.count()
            if count_opcoes > 1:
                logger.info("Dropdown detectado — clicando na opção 'Exportar'...")
                await opcoes.nth(1).click()
                await page.wait_for_timeout(2_000)

            logger.info("Exportação solicitada — aguardando 120s para processamento do servidor...")
            await page.wait_for_timeout(120_000)

            # 5. ABRIR PAINEL "ÚLTIMA TAREFA" via ícone de tarefas no header
            logger.info("Abrindo painel 'Última tarefa' via ícone de tarefas...")
            painel_aberto = False
            for tentativa_painel in range(4):
                try:
                    icone = page.locator('div[data-v-13320df0].icon').first
                    await icone.wait_for(timeout=5_000)
                    await icone.click()
                    await page.wait_for_timeout(3_000)
                    await page.screenshot(path=str(output_path / f"painel_tentativa_{tentativa_painel}.png"))
                    painel_aberto = True
                    logger.info(f"✅ Painel aberto (tentativa {tentativa_painel + 1})")
                    break
                except Exception as e:
                    logger.warning(f"Tentativa {tentativa_painel + 1} — ícone não encontrado: {e}")
                    await page.wait_for_timeout(30_000)

            if not painel_aberto:
                await page.screenshot(path=str(output_path / "erro_painel.png"))
                raise Exception("Não foi possível abrir o painel 'Última tarefa'.")

            # 6. AGUARDAR BOTÃO "BAIXAR" NO PAINEL (reabrindo para atualizar status)
            logger.info("Aguardando botão 'Baixar' no painel...")
            caminho_arquivo = None
            encontrado = False
            botao_baixar = None

            for tentativa_baixar in range(8):
                botao_baixar = page.locator('button:has-text("Baixar"), button:has-text("Download")').first
                try:
                    await botao_baixar.wait_for(timeout=30_000)
                    logger.info(f"✅ Botão 'Baixar' encontrado (tentativa {tentativa_baixar + 1})!")
                    encontrado = True
                    break
                except Exception:
                    elapsed_extra = (tentativa_baixar + 1) * 30
                    logger.info(f"Não visível ainda — reabrindo painel para atualizar ({elapsed_extra}s extra)...")
                    await page.screenshot(path=str(output_path / f"aguardando_baixar_{elapsed_extra}s.png"))
                    # Fecha o painel clicando fora e reabre para atualizar o status
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(2_000)
                    try:
                        icone = page.locator('div[data-v-13320df0].icon').first
                        await icone.wait_for(timeout=5_000)
                        await icone.click()
                        await page.wait_for_timeout(3_000)
                    except Exception as e:
                        logger.warning(f"Erro ao reabrir painel: {e}")

            if not encontrado:
                await page.screenshot(path=str(output_path / "erro_sem_baixar.png"))
                raise Exception("Timeout: botão 'Baixar' não apareceu no painel após 240s adicionais.")

            # 7. DOWNLOAD
            logger.info("Clicando em 'Baixar' no export mais recente...")
            async with page.expect_download(timeout=120_000) as download_info:
                await botao_baixar.click()

            download = await download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_arquivo = output_path / f"shopee_pnr_{timestamp}_{download.suggested_filename}"
            await download.save_as(str(caminho_arquivo))
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
            arquivo_interno = nomes[0]
            z.extractall(output_path)
        caminho_interno = output_path / arquivo_interno
        ext_interna = Path(arquivo_interno).suffix.lower()
        if ext_interna == ".csv":
            df = pd.read_csv(caminho_interno)
        else:
            df = pd.read_excel(caminho_interno)
    elif sufixo == ".csv":
        df = pd.read_csv(caminho_arquivo)
    else:
        df = pd.read_excel(caminho_arquivo)

    logger.info(f"Linhas brutas: {len(df)} | Colunas: {len(df.columns)}")

    # Extrair driver_id e nome do motorista (formato: "[123456] Nome do Motorista")
    col_motorista = next((c for c in df.columns if "motorista" in c.lower() or "driver" in c.lower()), None)
    if col_motorista:
        logger.info(f"Extraindo driver_id da coluna '{col_motorista}'...")
        extracao = df[col_motorista].astype(str).str.extract(r"\[(.*?)\]\s*(.*)")
        df.insert(0, "driver_id", extracao[0].fillna(""))
        df[col_motorista] = extracao[1].fillna(df[col_motorista])
    else:
        logger.warning("Coluna de motorista não encontrada — driver_id não extraído.")

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
    logger.info(f"Total Tickets PNR: {len(df)}")

    processed_file = output_path / f"processed_{timestamp}.csv"
    df.to_csv(processed_file, index=False)
    logger.info(f"Dados processados salvos: {processed_file}")

    return processed_file


async def run():
    try:
        arquivo = await extract_shopee_pnr()
        logger.info(f"✅ Extração concluída: {arquivo}")
        return str(arquivo)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
