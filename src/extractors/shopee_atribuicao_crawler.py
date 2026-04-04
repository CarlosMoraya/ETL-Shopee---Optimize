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

            # 3. SELECIONAR TODOS OS REGISTROS
            # Pelo screenshot: clicar no checkbox do header da tabela para abrir dropdown
            logger.info("Selecionando todos os registros...")
            try:
                # Aguardar a tabela carregar completamente
                await page.locator('table').first.wait_for(timeout=20_000)
                await page.wait_for_timeout(3_000)
                
                # Clicar no checkbox do header (canto superior esquerdo da tabela)
                # Tentar múltiplas abordagens
                checkbox_header = None
                for seletor in [
                    '.ant-table-selection-col input[type="checkbox"]',
                    'thead input[type="checkbox"]',
                    'th input[type="checkbox"]',
                    'table thead th:first-child input',
                    'div.ant-table-thead input[type="checkbox"]',
                ]:
                    try:
                        checkbox_header = page.locator(seletor).first
                        await checkbox_header.wait_for(timeout=5_000)
                        logger.info(f"Checkbox do header encontrado: {seletor}")
                        break
                    except Exception:
                        continue
                
                if checkbox_header:
                    await checkbox_header.click()
                    await page.wait_for_timeout(2_000)
                    
                    # Agora clicar em "Select All in All Pages"
                    logger.info("Clicando em 'Select All in All Pages'...")
                    # O dropdown é customizado e pode não ser "visível" para o Playwright
                    # Usar force=True para ignorar verificação de visibilidade
                    opcao_all = page.locator('text="Select All in All Pages"').first
                    await opcao_all.click(force=True, timeout=10_000)
                    await page.wait_for_timeout(5_000)
                    logger.info("✅ 'Select All in All Pages' clicado.")
                    
                    # Verificar se registros foram realmente selecionados
                    # Procurar por indicador como "X Task(s) Selected"
                    try:
                        texto_selecionados = await page.locator('text=Selected').first.text_content(timeout=5_000)
                        logger.info(f"Indicador de seleção: {texto_selecionados}")
                    except Exception:
                        logger.warning("Não foi possível verificar quantos registros foram selecionados.")
                    
                    # Tirar screenshot para confirmar
                    await page.screenshot(path=str(output_path / "pos_selecao.png"))
                    logger.info("✅ Todos os registros selecionados.")
                else:
                    logger.warning("Checkbox do header não encontrado — pulando seleção.")
            except Exception as e:
                logger.warning(f"Erro ao selecionar todos: {e}")
            
            await page.screenshot(path=str(output_path / "pos_select_all.png"))

            # 4. CLICAR EM "EXPORTAR AT"
            logger.info("Clicando em 'Exportar AT'...")
            
            # Tentar via JavaScript primeiro (mais confiável para este botão)
            clicado = await page.evaluate("""() => {
                const buttons = Array.from(document.querySelectorAll('button'));
                const exportBtn = buttons.find(btn => btn.textContent.trim().startsWith('Exportar AT'));
                if (exportBtn) {
                    exportBtn.click();
                    return true;
                }
                return false;
            }""")
            
            if clicado:
                logger.info("✅ Botão 'Exportar AT' clicado via JavaScript.")
            else:
                # Fallback: tentar seletores Playwright
                botao_exportar = None
                for seletor in [
                    'button:has-text("Exportar AT")',
                    'button.ssc-react-button >> text="Exportar AT"',
                ]:
                    try:
                        botao_exportar = page.locator(seletor).first
                        await botao_exportar.wait_for(timeout=5_000)
                        await botao_exportar.click()
                        logger.info(f"✅ Botão clicado via seletor: {seletor}")
                        break
                    except Exception:
                        continue
                
                if not botao_exportar:
                    logger.error("Botão 'Exportar AT' não encontrado!")
                    await page.screenshot(path=str(output_path / "erro_botao_exportar.png"))
                    raise Exception("Botão 'Exportar AT' não encontrado.")

            # Aguardar 30 segundos para o export ser processado
            logger.info("Aguardando 30 segundos para processamento do export...")
            await page.wait_for_timeout(30_000)
            logger.info("✅ Aguardo concluído.")

            # 5. ABRIR PAINEL "ÚLTIMA TAREFA" via ícone de tarefas no header
            logger.info("Abrindo painel 'Última tarefa' via ícone de tarefas...")
            painel_aberto = False
            for tentativa_painel in range(4):
                try:
                    icone = page.locator('div[data-v-13320df0].icon').first
                    await icone.wait_for(timeout=5_000)
                    await icone.click()
                    await page.wait_for_timeout(3_000)
                    
                    # Clicar em "Ver tudo" para abrir Export Task Center
                    logger.info("Clicando em 'Ver tudo'...")
                    try:
                        ver_tudo = page.locator('button:has-text("Ver tudo")').first
                        await ver_tudo.wait_for(timeout=10_000)
                        await ver_tudo.click()
                        await page.wait_for_timeout(3_000)
                    except Exception as e:
                        logger.warning(f"'Ver tudo' não encontrado: {e}")
                    
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

            # 6. AGUARDAR BOTÃO "BAIXAR" NO PAINEL
            logger.info("Aguardando botão 'Baixar' no painel...")
            
            # O painel pode precisar de scroll ou o botão pode estar em outra posição
            # Tentar múltiplos seletores
            botao_baixar = None
            encontrado = False
            
            for tentativa_baixar in range(4):
                try:
                    # Tentar múltiplos seletores para o botão Baixar
                    for seletor in [
                        'button:has-text("Baixar")',
                        'button.ssc-button:has-text("Baixar")',
                        'button.ssc-react-button:has-text("Baixar")',
                        'div >> text="Baixar"',
                    ]:
                        try:
                            botao_baixar = page.locator(seletor).first
                            await botao_baixar.wait_for(timeout=10_000)
                            logger.info(f"Botão 'Baixar' encontrado com: {seletor}")
                            encontrado = True
                            break
                        except Exception:
                            continue
                    
                    if encontrado:
                        break
                except Exception:
                    pass
                
                elapsed_extra = (tentativa_baixar + 1) * 30
                logger.info(f"Botão 'Baixar' não visível ainda — aguardando mais 30s ({elapsed_extra}s extra)...")
                await page.screenshot(path=str(output_path / f"aguardando_baixar_{elapsed_extra}s.png"))
                await page.wait_for_timeout(30_000)

            if not encontrado:
                await page.screenshot(path=str(output_path / "erro_sem_baixar.png"))
                raise Exception("Timeout: botão 'Baixar' não apareceu no painel.")

            # 7. DOWNLOAD — clica no PRIMEIRO botão "Baixar" (mais recente)
            logger.info("Clicando em 'Baixar' no export mais recente...")
            async with page.expect_download(timeout=120_000) as download_info:
                await botao_baixar.click()

            download = await download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}_{download.suggested_filename}"
            await download.save_as(str(caminho_arquivo))
            logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")

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
