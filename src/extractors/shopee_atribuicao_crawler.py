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
            try:
                await page.locator('text="Força de trabalho"').wait_for(timeout=30_000)
                logger.info("✅ Login confirmado — menu principal carregado!")
            except Exception:
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
            except Exception as e:
                logger.warning(f"Dropdown de seleção não encontrado: {e}")
                await page.screenshot(path=str(output_path / "erro_select_all_pages.png"))

            # 4. CLICAR EM "EXPORTAR AT"
            logger.info("Clicando em 'Exportar AT'...")
            try:
                botao_exportar = page.locator('button:has-text("Exportar AT")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()
            except Exception:
                # Fallback: tenta somente "Exportar"
                botao_exportar = page.locator('button:has-text("Exportar")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()

            await page.wait_for_timeout(2_000)

            # Verifica se abriu dropdown (segunda ocorrência)
            opcoes = page.locator('text=Exportar AT')
            count_opcoes = await opcoes.count()
            if count_opcoes > 1:
                logger.info("Dropdown detectado — clicando na opção 'Exportar AT'...")
                await opcoes.nth(1).click()
                await page.wait_for_timeout(2_000)

            logger.info("Exportação solicitada — aguardando 90s para processamento do servidor...")
            await page.wait_for_timeout(90_000)

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
            # Estratégia dual: espera evento de download (60s) + fallback de download manual via URL
            logger.info("Clicando em 'Baixar' no export mais recente...")
            download_holder = {"value": None}
            download_ready = asyncio.Event()
            all_request_urls = []

            async def on_download_event(dl):
                if not download_ready.is_set():
                    download_holder["value"] = dl
                    download_ready.set()
                    logger.info(f"Evento download capturado: {dl.suggested_filename}")

            def on_any_request(req):
                all_request_urls.append((req.resource_type, req.url))

            async def on_new_page(np):
                np.once("download", on_download_event)
                np.on("request", on_any_request)

            page.once("download", on_download_event)
            context.on("page", on_new_page)
            page.on("request", on_any_request)

            await botao_baixar.click()
            await page.wait_for_timeout(5_000)
            await page.screenshot(path=str(output_path / "pos_clique_baixar.png"))

            # Aguarda evento de download por 60s
            try:
                await asyncio.wait_for(download_ready.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            finally:
                context.remove_listener("page", on_new_page)
                page.remove_listener("request", on_any_request)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            if download_ready.is_set():
                # Caminho feliz: evento de download capturado
                download = download_holder["value"]
                caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}_{download.suggested_filename}"
                await download.save_as(str(caminho_arquivo))
                logger.info(f"✅ Arquivo baixado via evento: {caminho_arquivo}")
            else:
                # Fallback: tenta baixar manualmente via URL capturada
                logger.info(f"Evento não capturado. Requests após clique ({len(all_request_urls)}):")
                for rtype, url in all_request_urls[-30:]:
                    logger.info(f"  [{rtype}] {url}")

                dl_url = next(
                    (url for rtype, url in reversed(all_request_urls)
                     if any(kw in url.lower() for kw in ['download', 'export', '.csv', '.xlsx', '.zip', 'file'])),
                    None,
                )
                if dl_url:
                    # A URL capturada pode ser um endpoint de API que retorna JSON com a URL real do arquivo
                    logger.info(f"Consultando endpoint: {dl_url}")
                    api_resp = await page.request.get(dl_url, timeout=30_000)
                    if not api_resp.ok:
                        raise Exception(f"Endpoint falhou — status {api_resp.status}: {dl_url}")

                    content_type = api_resp.headers.get("content-type", "")
                    logger.info(f"Content-Type da resposta: {content_type}")

                    if "json" in content_type:
                        # É JSON com lista de tarefas de export
                        # Estrutura: {"data": {"exports": [{"filename": "downloads/export/.../arquivo.csv", ...}]}}
                        resp_json = await api_resp.json()
                        logger.info(f"Resposta JSON: {str(resp_json)[:500]}")

                        exports = resp_json.get("data", {}).get("exports", [])
                        if not exports:
                            raise Exception(f"Nenhuma tarefa de export encontrada no JSON: {resp_json}")

                        # Pega o export mais recente (primeiro da lista, ordenado por ctime desc)
                        filename_relativo = exports[0].get("filename", "")
                        if not filename_relativo:
                            raise Exception(f"Campo 'filename' vazio no export: {exports[0]}")

                        file_url = f"{PORTAL_URL.rstrip('/')}/{filename_relativo.lstrip('/')}"
                        logger.info(f"URL real do arquivo: {file_url}")

                        file_resp = await page.request.get(file_url, timeout=300_000)
                        if not file_resp.ok:
                            raise Exception(f"Download do arquivo falhou — status {file_resp.status}: {file_url}")
                        content_type = file_resp.headers.get("content-type", "")
                        ext = Path(filename_relativo).suffix or (
                            ".zip" if "zip" in content_type else ".csv" if "csv" in content_type else ".xlsx"
                        )
                        caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
                        caminho_arquivo.write_bytes(await file_resp.body())
                        logger.info(f"✅ Arquivo baixado via JSON: {caminho_arquivo}")
                    else:
                        # Resposta já é o arquivo direto
                        ext = ".zip" if "zip" in content_type else ".csv" if "csv" in content_type else ".xlsx"
                        caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
                        caminho_arquivo.write_bytes(await api_resp.body())
                        logger.info(f"✅ Arquivo baixado diretamente: {caminho_arquivo}")
                else:
                    raise Exception(
                        "Timeout: nem evento de download nem URL de download capturados. "
                        f"Requests detectadas: {[u for _, u in all_request_urls[-10:]]}"
                    )

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
