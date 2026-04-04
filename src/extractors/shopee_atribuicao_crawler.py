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

            # 4. REGISTRAR EXPORTS EXISTENTES antes de disparar novo export
            HISTORY_URL = (
                "https://logistics.myagencyservice.com.br"
                "/api/delivery/agency/assignment/assignment_task/export/history"
            )
            logger.info("Registrando exports existentes antes de exportar...")
            hist_antes = await page.request.get(HISTORY_URL, timeout=30_000)
            hist_json = await hist_antes.json()
            existing_task_ids = {
                e["task_id"] for e in hist_json.get("data", {}).get("exports", [])
            }
            logger.info(f"Task IDs existentes: {existing_task_ids}")

            # 5. DISPARAR EXPORT — capturar TODOS os requests
            logger.info("Configurando interceptação de TODOS os requests...")
            all_requests = []
            
            async def on_request(request):
                all_requests.append({
                    "url": request.url,
                    "method": request.method,
                    "resource_type": request.resource_type,
                    "post_data": request.post_data[:500] if request.post_data else None
                })
            
            page.on("request", on_request)
            
            logger.info("Clicando em 'Exportar AT'...")
            botao_exportar = page.locator('button:has-text("Exportar AT")').first
            await botao_exportar.wait_for(timeout=10_000)
            await botao_exportar.click()
            await page.wait_for_timeout(8_000)

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
                        await page.wait_for_timeout(5_000)
                        break
                except Exception:
                    pass

            await page.wait_for_timeout(5_000)
            
            # Parar interceptação
            page.remove_listener("request", on_request)
            
            # Log de TODOS os requests
            logger.info(f"TOTAL de requests capturados: {len(all_requests)}")
            for i, req in enumerate(all_requests):
                logger.info(f"  [{i}] {req['method']} {req['url']} (type={req['resource_type']})")
                if req.get('post_data'):
                    logger.info(f"       POST: {req['post_data'][:300]}")
            
            # Filtrar apenas requests XHR/fetch (API calls)
            api_requests = [r for r in all_requests if r['resource_type'] in ('xhr', 'fetch')]
            logger.info(f"Requests de API (XHR/fetch): {len(api_requests)}")
            for req in api_requests:
                logger.info(f"  → {req['method']} {req['url']}")
                if req.get('post_data'):
                    logger.info(f"     Payload: {req['post_data']}")
            
            await page.screenshot(path=str(output_path / "pos_exportar_at.png"))

            # Verificar se o clique criou um novo export
            logger.info("Verificando se export foi criado via clique...")
            hist_pos_clique = await page.request.get(HISTORY_URL, timeout=30_000)
            hist_pos_clique_json = await hist_pos_clique.json()
            exports_pos_clique = hist_pos_clique_json.get("data", {}).get("exports", [])
            novos_pos_clique = [e for e in exports_pos_clique if e["task_id"] not in existing_task_ids]
            
            export_criado = len(novos_pos_clique) > 0
            
            if export_criado:
                logger.info(f"✅ Export criado via clique: {novos_pos_clique[0]}")
            else:
                logger.error("❌ Falha ao criar export — nenhum novo task_id detectado após clique no botão.")
                logger.error(f"Total de requests capturados: {len(all_requests)}, API requests: {len(api_requests)}")
                
                if not api_requests:
                    logger.error("NENHUM request de API foi capturado!")
                    logger.error("Possíveis causas:")
                    logger.error("  1. Botão não está sendo clicado corretamente")
                    logger.error("  2. JavaScript da página está com erro")
                    logger.error("  3. Página precisa de interação adicional")
                    
                    # Tentar avaliar JavaScript da página
                    logger.info("Avaliando estado da página via JavaScript...")
                    try:
                        page_title = await page.title()
                        logger.info(f"Título da página: {page_title}")
                        current_url = page.url
                        logger.info(f"URL atual: {current_url}")
                    except Exception as e:
                        logger.error(f"Erro ao avaliar página: {e}")
                
                await page.screenshot(path=str(output_path / "erro_export_falhado.png"))
                raise Exception("Falha ao criar export: botão não gerou novo task_id. Verifique logs de requests.")

            # 6. ABRIR PAINEL → VER TUDO (navegação como humano)
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
            logger.info("✅ Export Task Center aberto.")

            # 7. POLLING DA API para identificar o novo export de AT (ignora Romaneio e outros)
            # Aguarda até 25 minutos (150 tentativas × 10s)
            logger.info("Aguardando novo export de AT ficar pronto...")
            novo_export = None
            caminho_arquivo = None
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            status_detectados = set()

            for tentativa in range(150):
                await page.wait_for_timeout(10_000)
                elapsed = (tentativa + 1) * 10
                try:
                    hist_resp = await page.request.get(HISTORY_URL, timeout=30_000)
                    hist_json = await hist_resp.json()
                    exports = hist_json.get("data", {}).get("exports", [])
                    
                    # Log da primeira resposta completa para debugging
                    if tentativa == 0:
                        logger.info(f"API response (todos os exports): {exports[:5]}...")
                    
                    novos = [e for e in exports if e["task_id"] not in existing_task_ids]
                    if novos:
                        # Log de todos os status detectados para debugging
                        for e in novos:
                            status_detectados.add(e.get("status"))
                        
                        # Log detalhado de novos exports
                        logger.info(f"Novos exports detectados: {[{'task_id': e['task_id'], 'status': e.get('status'), 'filename': e.get('filename', '')} for e in novos]}")

                        # status == 2 = pronto, status == 1 = processando, status == 3 = falhou
                        concluido = next((e for e in novos if e.get("status") == 2), None)
                        falhou = next((e for e in novos if e.get("status") in [3, 4, 5]), None)

                        if falhou:
                            logger.error(f"❌ Export falhou com status={falhou.get('status')} — task_id={falhou['task_id']}")
                            raise Exception(f"Export falhou com status={falhou.get('status')} — task_id={falhou['task_id']}")

                        if concluido:
                            novo_export = concluido
                            logger.info(f"✅ Novo export pronto após {elapsed}s — task_id={novo_export['task_id']}")
                        else:
                            logger.info(f"Novo export detectado mas ainda processando (status={[e.get('status') for e in novos]}) — {elapsed}s")
                    else:
                        if tentativa % 6 == 0:  # Log a cada ~60s
                            logger.info(f"Aguardando novo export... {elapsed}s decorridos")
                            # Log dos últimos 3 exports existentes para ver se API está respondendo
                            ultimos_3 = exports[:3] if len(exports) >= 3 else exports
                            logger.info(f"Últimos exports conhecidos: {[(e.get('task_id'), e.get('status')) for e in ultimos_3]}")
                except Exception as e:
                    logger.warning(f"Erro ao consultar history ({elapsed}s): {e}")

                if novo_export:
                    break

            if not novo_export:
                logger.error(f"Status detectados durante polling: {status_detectados}")
                raise Exception("Timeout: novo export de AT não ficou pronto em 25 minutos.")

            # 8. BAIXAR O ARQUIVO CORRETO via URL identificada no polling
            filename_relativo = novo_export.get("filename", "")
            if not filename_relativo:
                raise Exception(f"Campo 'filename' vazio no export: {novo_export}")

            file_url = f"{PORTAL_URL.rstrip('/')}/{filename_relativo.lstrip('/')}"
            logger.info(f"Baixando arquivo correto: {file_url}")

            file_resp = await page.request.get(file_url, timeout=300_000)
            if not file_resp.ok:
                raise Exception(f"Download falhou — status {file_resp.status}: {file_url}")

            content_type = file_resp.headers.get("content-type", "")
            ext = Path(filename_relativo).suffix or (
                ".zip" if "zip" in content_type else ".csv" if "csv" in content_type else ".xlsx"
            )
            caminho_arquivo = output_path / f"shopee_atribuicao_{timestamp}{ext}"
            caminho_arquivo.write_bytes(await file_resp.body())
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
