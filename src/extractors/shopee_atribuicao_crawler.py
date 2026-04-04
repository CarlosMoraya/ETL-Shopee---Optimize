"""
Extractor: Atribuição de Entrega
Fonte: Shopee Logistics - Login via Playwright (navegação real no portal)
Destino: data/raw/shopee_atribuicao/processed_*.csv

Fluxo baseado no passo a passo manual funcional:
1. Login no portal
2. Navegar para "Atribuição de Entrega"
3. Clicar em "Todos" (tab)
4. Clicar no dropdown "Todos" → "Select All in All Pages"
5. Clicar em "Exportar AT"
6. Aguardar 30 segundos
7. Abrir painel "Última tarefa" e clicar "Baixar"
8. Tratar com pandas
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
            login_url = (
                "https://accounts.myagencyservice.com.br/authenticate/login?"
                "lang=pt-BR&should_hide_back=true&client_id=15&"
                "next=https%3A%2F%2Flogistics.myagencyservice.com.br%2Fauth%2Fcallback"
                "%3Frefer%3Dhttps%3A%2F%2Flogistics.myagencyservice.com.br%2F%23%2Fagency-assignment%2Flist"
            )
            logger.info(f"Acessando página de login: {login_url[:80]}...")
            await page.goto(login_url, wait_until="networkidle", timeout=60_000)

            logger.info("Preenchendo email...")
            email_input = page.locator('input[autocomplete="email"]').first
            await email_input.wait_for(timeout=30_000)
            await email_input.fill(email)

            logger.info("Preenchendo senha...")
            senha_input = page.locator('input[type="password"]').first
            await senha_input.fill(senha)

            logger.info("Clicando no botão de login...")
            # Tentar múltiplos seletores para o botão de login
            botao_login = None
            for seletor in [
                'button[type="submit"]',
                'button.ssc-button',
                'form button',
                'button:has-text("Login")',
                'button:has-text("Entrar")',
            ]:
                try:
                    botao_login = page.locator(seletor).first
                    await botao_login.wait_for(timeout=5_000)
                    logger.info(f"Botão encontrado com seletor: {seletor}")
                    break
                except Exception:
                    continue
            
            if not botao_login:
                # Último recurso: usar XPath aproximado
                botao_login = page.locator('xpath=//form//button').first
                await botao_login.wait_for(timeout=10_000)
                logger.info("Botão encontrado via XPath genérico")
            
            await botao_login.click()

            logger.info("Aguardando portal carregar após login...")
            # A página de login redireciona direto para /#/agency-assignment/list
            # Verificar se a página carregou corretamente
            await page.wait_for_load_state("domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(5_000)
            
            # Tirar screenshot para debug
            await page.screenshot(path=str(output_path / "pos_login.png"))
            
            # Verificar se estamos na página correta
            current_url = page.url
            logger.info(f"URL atual após login: {current_url}")
            
            # Confirmar login bem-sucedido verificando elementos da página
            login_confirmado = False
            for seletor in [
                'text="Atribuição de Entrega"',
                'text="Força de trabalho"',
                'text="Entrega LM"',
                '.nav-menu',
                '.sidebar',
                'table',
            ]:
                try:
                    await page.locator(seletor).first.wait_for(timeout=10_000)
                    logger.info(f"✅ Login confirmado — elemento '{seletor}' encontrado!")
                    login_confirmado = True
                    break
                except Exception:
                    pass
            
            if not login_confirmado:
                logger.error(f"URL atual: {page.url}")
                raise Exception("Login pode ter falhado — nenhum elemento esperado encontrado na página.")

            # 2. NAVEGAR PARA ATRIBUIÇÃO DE ENTREGA
            logger.info(f"Navegando para: {ATRIBUICAO_URL}")
            await page.goto(ATRIBUICAO_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(15_000)
            await page.screenshot(path=str(output_path / "pagina_atribuicao.png"))
            logger.info("✅ Página de Atribuição de Entrega carregada.")

            # 3. CLICAR NA TAB "TODOS"
            logger.info("Clicando na tab 'Todos'...")
            try:
                # Tentar múltiplos seletores para a tab "Todos"
                tab_todos = None
                for seletor in [
                    'text="Todos"',
                    'a:has-text("Todos")',
                    'button:has-text("Todos")',
                    '.ant-tabs-tab:has-text("Todos")',
                    '.nav-item:has-text("Todos")',
                ]:
                    try:
                        tab_todos = page.locator(seletor).first
                        await tab_todos.wait_for(timeout=5_000)
                        logger.info(f"Tab encontrada com seletor: {seletor}")
                        break
                    except Exception:
                        continue
                
                if tab_todos:
                    await tab_todos.click()
                    await page.wait_for_timeout(2_000)
                    logger.info("✅ Tab 'Todos' clicada.")
                else:
                    logger.warning("Tab 'Todos' não encontrada — pode já estar selecionada.")
            except Exception as e:
                logger.warning(f"Erro ao clicar tab 'Todos': {e}")
            
            await page.screenshot(path=str(output_path / "pos_tab_todos.png"))

            # 4. CLICAR NO DROPDOWN "TODOS" → "SELECT ALL IN ALL PAGES"
            logger.info("Clicando no dropdown de seleção da tabela...")
            try:
                # Pelo screenshot: o dropdown fica no header da tabela, próximo ao checkbox
                # Tentar múltiplos seletores
                dropdown_todos = None
                for seletor in [
                    '.ssc-react-table-selection-menu-trigger',
                    '.table-selection-trigger',
                    'div[role="button"]:has(svg)',
                    'span:has(svg).ant-dropdown-trigger',
                    '.ant-table-selection-col .ant-dropdown-trigger',
                ]:
                    try:
                        dropdown_todos = page.locator(seletor).first
                        await dropdown_todos.wait_for(timeout=5_000)
                        logger.info(f"Dropdown encontrado com seletor: {seletor}")
                        break
                    except Exception:
                        continue
                
                if dropdown_todos:
                    await dropdown_todos.click()
                    await page.wait_for_timeout(1_000)
                    
                    logger.info("Clicando em 'Select All in All Pages'...")
                    opcao_all_pages = page.locator('text="Select All in All Pages"').first
                    await opcao_all_pages.click()
                    await page.wait_for_timeout(3_000)
                    logger.info("✅ 'Select All in All Pages' selecionado.")
                else:
                    logger.warning("Dropdown não encontrado — pulando seleção.")
            except Exception as e:
                logger.warning(f"Erro ao selecionar todos: {e}")
                await page.screenshot(path=str(output_path / "erro_select_all.png"))
            
            await page.screenshot(path=str(output_path / "pos_select_all.png"))

            # 5. CLICAR EM "EXPORTAR AT"
            logger.info("Clicando em 'Exportar AT'...")
            botao_exportar = None
            for seletor in [
                'button:has-text("Exportar AT")',
                'button.ssc-button:has-text("Exportar")',
                '.ant-btn:has-text("Exportar AT")',
                'div[role="button"]:has-text("Exportar AT")',
                'button:has-text("Exportar")',
            ]:
                try:
                    botao_exportar = page.locator(seletor).first
                    await botao_exportar.wait_for(timeout=5_000)
                    logger.info(f"Botão Exportar AT encontrado com seletor: {seletor}")
                    break
                except Exception:
                    continue
            
            if botao_exportar:
                await botao_exportar.click()
                logger.info("✅ Botão 'Exportar AT' clicado.")
            else:
                logger.error("Botão 'Exportar AT' não encontrado!")
                await page.screenshot(path=str(output_path / "erro_botao_exportar.png"))
                raise Exception("Botão 'Exportar AT' não encontrado na página.")

            # Tratar modal de confirmação, se aparecer
            await page.wait_for_timeout(2_000)
            for seletor_confirmar in [
                'button:has-text("Confirm")',
                'button:has-text("Confirmar")',
                'button:has-text("OK")',
                'button:has-text("Yes")',
                'button:has-text("Sim")',
            ]:
                try:
                    btn = page.locator(seletor_confirmar).first
                    if await btn.is_visible():
                        logger.info(f"Modal detectado — clicando '{seletor_confirmar}'")
                        await btn.click()
                        await page.wait_for_timeout(1_500)
                        break
                except Exception:
                    pass

            await page.screenshot(path=str(output_path / "pos_exportar_at.png"))

            # 6. AGUARDAR 30 SEGUNDOS
            logger.info("Aguardando 30 segundos para o export ser processado...")
            await page.wait_for_timeout(30_000)
            logger.info("✅ Aguardo concluído.")

            # 7. ABRIR PAINEL "ÚLTIMA TAREFA"
            logger.info("Abrindo painel 'Última tarefa'...")
            try:
                icone_tarefa = page.locator('.icon').filter(has_text="").first
                await icone_tarefa.wait_for(timeout=10_000)
                await icone_tarefa.click()
                await page.wait_for_timeout(2_000)
                
                logger.info("Clicando em 'Ver tudo'...")
                ver_tudo = page.locator('button:has-text("Ver tudo")').first
                await ver_tudo.click()
                await page.wait_for_timeout(3_000)
                await page.screenshot(path=str(output_path / "export_task_center.png"))
                logger.info("✅ Export Task Center aberto.")
            except Exception as e:
                logger.warning(f"Erro ao abrir painel: {e}")
                # Tentar alternativa: recarregar a página e tentar novamente
                logger.info("Tentando recarregar a página...")
                await page.goto(ATRIBUICAO_URL, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(5_000)

            # 8. BAIXAR O ARQUIVO MAIS RECENTE
            logger.info("Procurando botão 'Baixar' do export mais recente...")
            try:
                # Procurar pelo primeiro botão "Baixar" que esteja visível
                botoes_baixar = page.locator('button:has-text("Baixar")')
                await botoes_baixar.first.wait_for(timeout=30_000)
                
                # Clicar no primeiro botão "Baixar" (mais recente)
                botao_baixar = botoes_baixar.first
                await botao_baixar.click()
                logger.info("✅ Botão 'Baixar' clicado.")
                
                # Aguardar o download
                async with page.expect_download(timeout=300_000) as download_info:
                    pass  # O clique já foi feito
                
                download = await download_info.value
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                ext = Path(download.suggested_filename).suffix
                caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
                await download.save_as(caminho_arquivo)
                logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")
                
            except Exception as e:
                logger.error(f"Erro ao baixar: {e}")
                await page.screenshot(path=str(output_path / "erro_download.png"))
                
                # Tentativa alternativa: buscar via API
                logger.info("Tentando download via API...")
                HISTORY_URL = (
                    "https://logistics.myagencyservice.com.br"
                    "/api/delivery/agency/assignment/assignment_task/export/history"
                )
                hist_resp = await page.request.get(HISTORY_URL, timeout=30_000)
                hist_json = await hist_resp.json()
                exports = hist_json.get("data", {}).get("exports", [])
                
                if exports:
                    # Pegar o export mais recente de AT (não Romaneio)
                    export_at = next(
                        (e for e in exports if "romaneio" not in e.get("task_name", "").lower()),
                        exports[0]
                    )
                    filename_relativo = export_at.get("filename", "")
                    if filename_relativo:
                        file_url = f"{PORTAL_URL.rstrip('/')}/{filename_relativo.lstrip('/')}"
                        logger.info(f"Baixando via API: {file_url}")
                        file_resp = await page.request.get(file_url, timeout=300_000)
                        if file_resp.ok:
                            caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
                            caminho_arquivo.write_bytes(await file_resp.body())
                            logger.info(f"✅ Arquivo baixado via API: {caminho_arquivo}")
                        else:
                            raise Exception(f"Download via API falhou — status {file_resp.status}")
                    else:
                        raise Exception("Campo 'filename' vazio no export.")
                else:
                    raise Exception("Nenhum export encontrado na API.")

        finally:
            await browser.close()

    # 9. PROCESSAR COM PANDAS
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
