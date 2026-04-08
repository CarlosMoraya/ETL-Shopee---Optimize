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
import re

from playwright.async_api import async_playwright

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"
ATRIBUICAO_URL = "https://logistics.myagencyservice.com.br/#/agency-assignment/list"


async def extract_shopee_atribuicao() -> Path:
    import pandas as pd

    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")
    headless_env = os.environ.get("CRAWLER_HEADLESS", "true").lower() == "true"

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos nos secrets.")

    output_path = DATA_RAW_DIR / "shopee_atribuicao"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Atribuição de Entrega")
    logger.info("=" * 80)

    async with async_playwright() as p:
        logger.info(f"Iniciando navegador... (Headless: {headless_env})")
        browser = await p.chromium.launch(
            headless=headless_env,
            slow_mo=300 if not headless_env else 0, # Fica visual para o usuário quando false
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
            
            # Confirmar login bem-sucedido via URL ou elementos da página
            current_url_lower = current_url.lower()
            if "login" in current_url_lower:
                logger.error(f"Redirecionamento falhou, URL atual: {current_url}")
                await page.screenshot(path=str(output_path / "erro_login.png"))
                raise Exception("Login falhou — o sistema permaneceu na tela de login.")

            login_confirmado = False
            
            # Se a URL indicar o dashboard/listagem, consideramos sucesso no redirecionamento.
            if "agency-assignment" in current_url_lower or "logistics.myagencyservice.com.br/#/" in current_url_lower:
                logger.info("✅ Login confirmado — a URL indica acesso à área interna!")
                login_confirmado = True

            # Caso ainda queira validar o carregamento da interface:
            if not login_confirmado:
                for seletor in [
                    '.ant-layout',
                    '.ant-table',
                    '.ant-menu',
                    'table',
                    'text="Atribuição de Entrega"',
                    'text="Força de trabalho"',
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
                raise Exception("Login pode ter falhado — a URL não era a esperada e nenhum elemento esperado rendeu.")

            # 2. NAVEGAR PARA ATRIBUIÇÃO DE ENTREGA
            logger.info(f"Navegando para: {ATRIBUICAO_URL}")
            await page.goto(ATRIBUICAO_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(10_000)
            await page.screenshot(path=str(output_path / "pagina_atribuicao.png"))

            # 2.1. Clicar na Aba 'Todos'
            logger.info("Clicando na aba 'Todos'...")
            aba_encontrada = False
            try:
                # Tenta localizar a estrutura de abas
                await page.locator('.ant-tabs-tab, .tab-item, div[role="tab"]').first.wait_for(timeout=5_000)
                
                abas = await page.locator('.ant-tabs-tab, .tab-item, div[role="tab"]').all()
                for aba in abas:
                    try:
                        if await aba.is_visible():
                            texto_aba = await aba.inner_text()
                            if "Todos" in texto_aba or "All" in texto_aba:
                                await aba.click(force=True)
                                logger.info(f"✅ Aba '{texto_aba.strip()}' clicada via estrutura.")
                                await page.wait_for_timeout(5_000)
                                aba_encontrada = True
                                break
                    except:
                        pass
            except Exception as e:
                logger.warning(f"Estrutura padrão de abas não encontrada: {e}")
                
            if not aba_encontrada:
                try:
                    logger.info("Iniciando fallback: clicando aba via texto literal...")
                    aba_text = page.locator('text=/^(Todos|All)$/i').first
                    if await aba_text.count() > 0:
                        await aba_text.click(force=True)
                        logger.info("✅ Aba clicada via fallback de texto.")
                        await page.wait_for_timeout(5_000)
                    else:
                        logger.warning("Nenhuma aba com texto 'Todos' encontrada via fallback.")
                except Exception as e:
                    logger.warning(f"Aviso ao tentar fallback da aba Todos: {e}")

            # 2.2. Limpar Filtros de Data (se houver, para garantir full download)
            logger.info("Garantindo que os filtros de data estão limpos / selecionado o maior período...")
            try:
                clear_icons = await page.locator('.ant-picker-clear').all()
                for icon in clear_icons:
                    if await icon.is_visible():
                        await icon.click(force=True)
                        await page.wait_for_timeout(1_000)
            except Exception:
                pass

            # 2.3. ALTERAR PAGINAÇÃO PARA 100 (máximo) para garantir que mais itens fiquem nativos, se necessário
            logger.info("Configurando paginação para 100 itens por página...")
            try:
                dropdown_pagina = page.locator('text=/\\d+\\s*\\/\\s*página/').first
                await dropdown_pagina.wait_for(timeout=10_000)
                await dropdown_pagina.click(force=True)  # force=True resolve o pointer events intercept
                await page.wait_for_timeout(1_000)
                
                opcao_100 = page.locator('text="100"').first
                await opcao_100.click(force=True)
                await page.wait_for_timeout(3_000)
                logger.info("✅ Paginação configurada para 100 itens/página.")
            except Exception as e:
                logger.warning(f"Não foi possível alterar paginação: {e}")
            
            await page.screenshot(path=str(output_path / "pos_paginacao.png"))

            # 3. SELECIONAR TODOS OS REGISTROS
            logger.info("Selecionando todos os registros...")
            try:
                await page.locator('table').first.wait_for(timeout=20_000)
                await page.wait_for_timeout(3_000)
                
                checkbox_header = None
                for seletor in [
                    '.ant-table-selection-col input[type="checkbox"]',
                    'thead input[type="checkbox"]',
                    'th input[type="checkbox"]',
                ]:
                    try:
                        checkbox_header = page.locator(seletor).first
                        await checkbox_header.wait_for(timeout=5_000)
                        logger.info(f"Checkbox do header encontrado: {seletor}")
                        break
                    except Exception:
                        continue
                
                if checkbox_header:
                    # Clicar na seta/dropdown do lado do checkbox primeiro
                    try:
                        # Achamos o th (cabeçalho) que contém o checkbox, nele geralmente tem um ícone de dropdown
                        icone_dropdown = page.locator('th').filter(has=page.locator('input[type="checkbox"]')).locator('.ant-dropdown-trigger, i[class*="icon"], svg').last
                        if await icone_dropdown.count() > 0:
                            await icone_dropdown.click(force=True)
                            logger.info("Clicou no trigger do dropdown do checkbox.")
                    except:
                        pass
                    
                    await page.wait_for_timeout(2_000)
                    
                    logger.info("Procurando e clicando em 'Select All in All Pages'...")
                    clicado = await page.evaluate("""() => {
                        const items = Array.from(document.querySelectorAll('.ssc-react-table-selection-menu-item, div[role="option"], .ant-dropdown-menu-item, li[role="menuitem"]'));
                        const selectAllItem = items.find(item => 
                            item.textContent.includes('Select All in All Pages') ||
                            item.textContent.includes('Selecionar todos nas páginas') ||
                            item.textContent.includes('Selecionar todas as páginas')
                        );
                        if (selectAllItem) {
                            selectAllItem.click();
                            return true;
                        }
                        return false;
                    }""")
                    
                    if not clicado:
                        # Se não conseguiu abrir o dropdown e achar a opção, clica no checkbox normal como fallback
                        logger.info("Opção 'Select All in All Pages' não visível de primeira, fatiando clique normal no checkbox.")
                        await checkbox_header.click(force=True)
                        await page.wait_for_timeout(1_000)
                        
                        # E tenta achar novamente a opção que pode aparecer DEPOIS de selecionar a página atual
                        clicado = await page.evaluate("""() => {
                            const items = Array.from(document.querySelectorAll('.ssc-react-table-selection-menu-item, div[role="option"], .ant-dropdown-menu-item, li[role="menuitem"]'));
                            const selectAllItem = items.find(item => 
                                item.textContent.includes('Select All in All Pages') ||
                                item.textContent.includes('Selecionar todos nas páginas') ||
                                item.textContent.includes('Selecionar todas as páginas')
                            );
                            if (selectAllItem) {
                                selectAllItem.click();
                                return true;
                            }
                            return false;
                        }""")
                    
                    if clicado:
                        logger.info("✅ Seleção em lote (All Pages) clicada.")
                    else:
                        try:
                            await page.locator('text=/Select All in All Pages|Selecionar todos nas/').first.click(force=True, timeout=5_000)
                            logger.info("✅ Seleção em lote clicada via Playwright.")
                        except Exception as e:
                            logger.warning(f"Falha tentar localizar em lote: {e}")
                    
                    await page.wait_for_timeout(5_000)
                    try:
                        texto_final = await page.locator('text=Selected').first.text_content(timeout=5_000)
                        logger.info(f"Total selecionado: {texto_final}")
                    except:
                        pass
                else:
                    logger.warning("Checkbox do header não encontrado.")
            except Exception as e:
                logger.warning(f"Erro ao selecionar: {e}")
            
            await page.screenshot(path=str(output_path / "pos_select_all.png"))

            # 4. CLICAR EM "EXPORTAR AT" (evitando botões por linha da tabela)
            logger.info("Clicando em 'Exportar' em lote...")
            clicado = await page.evaluate("""() => {
                const buttons = Array.from(document.querySelectorAll('button:not(:disabled)'));
                // Filtramos EXPLICITAMENTE os botões que não estão dentro da tabela
                // pois cada linha pode ter um botão de "Exportar AT" (o que baixa somente 1)
                const bulkButtons = buttons.filter(btn => !btn.closest('table') && !btn.closest('.ant-table'));
                
                const exportBtn = bulkButtons.find(btn => 
                    btn.textContent.trim().startsWith('Exportar AT') ||
                    btn.textContent.trim().startsWith('Exportar') ||
                    btn.textContent.trim() === 'Export'
                );
                
                if (exportBtn) {
                    exportBtn.click();
                    return true;
                }
                return false;
            }""")
            
            if clicado:
                logger.info("✅ Botão de Exportação em Lote clicado via JavaScript!")
            elif clicado:
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
                    
                    # O portal alterna entre PT-BR e EN; aceitar ambos.
                    logger.info("Tentando abrir detalhes completos da tarefa...")
                    try:
                        ver_tudo = None
                        for seletor_ver_tudo in [
                            'button:has-text("Ver tudo")',
                            'button:has-text("View All")',
                        ]:
                            try:
                                ver_tudo = page.locator(seletor_ver_tudo).first
                                await ver_tudo.wait_for(timeout=5_000)
                                await ver_tudo.click()
                                logger.info(f"Detalhes abertos com seletor: {seletor_ver_tudo}")
                                await page.wait_for_timeout(3_000)
                                break
                            except Exception:
                                continue
                    except Exception as e:
                        logger.warning(f"Botão de detalhes completos não encontrado: {e}")
                    
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

            # 6. AGUARDAR E CLICAR NO BOTÃO "DOWNLOAD" DA TAREFA MAIS RECENTE
            logger.info("Procurando botão 'Download' da tarefa mais recente...")
            
            # Usar JavaScript para encontrar o botão Download da primeira task-row (mais recente)
            for tentativa in range(6):
                try:
                    clicado = await page.evaluate("""() => {
                        // Pegar a PRIMEIRA task-row (mais recente) e clicar seu botão Download
                        const taskRows = document.querySelectorAll('.task-row');
                        if (taskRows.length > 0) {
                            const firstRow = taskRows[0];
                            const downloadBtn = firstRow.querySelector('.status-wrapper button');
                            if (downloadBtn && downloadBtn.offsetParent !== null) {
                                // Verificar se é um botão Download/Baixar
                                const text = downloadBtn.textContent.trim();
                                if (text === 'Download' || text === 'Baixar') {
                                    downloadBtn.click();
                                    return true;
                                }
                            }
                        }
                        return false;
                    }""")
                    
                    if clicado:
                        logger.info(f"✅ Botão 'Download' clicado via JavaScript (tentativa {tentativa + 1})")
                        break
                except Exception as e:
                    logger.warning(f"Erro ao clicar Download: {e}")
                
                # Aguardar e tentar novamente
                await page.wait_for_timeout(30_000)
                logger.info(f"Aguardando export processar... {tentativa + 1} tentativa(s)")
            else:
                await page.screenshot(path=str(output_path / "erro_sem_download.png"))
                raise Exception("Timeout: botão 'Download' não encontrado.")

            # 7. CAPTURAR DOWNLOAD
            logger.info("Aguardando download...")
            async with page.expect_download(timeout=120_000) as download_info:
                pass  # O clique já foi feito via JavaScript

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

    # Adicionar a nova coluna 'embarcador' na primeira posição
    df.insert(0, "embarcador", "Shopee_Last_Mile")

    logger.info(f"Colunas normalizadas: {list(df.columns)}")
    logger.info(f"Total Atribuições: {len(df)}")

    # Salvar DataFrame completo
    processed_file_completo = output_path / f"processed_{timestamp}_complet.csv"
    df.to_csv(processed_file_completo, index=False)
    logger.info(f"Dados processados (completo) salvos: {processed_file_completo}")

    # Gerar DataFrame focado apenas em ATs únicas
    # Possíveis nomes após normalização para a coluna de AT
    possiveis_nomes_at = ["assignment_task_id", "at", "id_da_at", "task_id"]
    coluna_at = next((col for col in possiveis_nomes_at if col in df.columns), None)
    
    # Se não encontrar pelos nomes conhecidos, usar a primeira que possa ter 'task_id' ou 'at' no nome
    if not coluna_at:
        coluna_at = next((col for col in df.columns if "task_id" in col or "at_" in col or "_at" in col), None)

    if coluna_at:
        logger.info(f"Removendo duplicatas baseado na coluna da AT: '{coluna_at}'")
        df_uniq = df.drop_duplicates(subset=[coluna_at]).copy()
    else:
        logger.warning("Não foi possível identificar a coluna de identificação da AT. Usando todas as linhas no df_uniq.")
        df_uniq = df.copy()

    # Remover a coluna com número de pedido, se existir (várias possiblidades de digitação/normalização)
    colunas_pedido = [c for c in df_uniq.columns if "nmero_do_pedido" in c or "numero_do_pedido" in c or "nmero_pedido" in c or "pedido" in c]
    if colunas_pedido:
        df_uniq = df_uniq.drop(columns=colunas_pedido)
        logger.info(f"Colunas descartadas do df_uniq: {colunas_pedido}")

    processed_file_uniq = output_path / f"processed_{timestamp}_uniq.csv"
    df_uniq.to_csv(processed_file_uniq, index=False)
    logger.info(f"Dados processados (únicos) salvos: {processed_file_uniq}")

    return {
        "completo": str(processed_file_completo),
        "uniq": str(processed_file_uniq)
    }

async def run():
    try:
        arquivos = await extract_shopee_atribuicao()
        logger.info(f"✅ Extração concluída. Arquivos gerados:")
        logger.info(f" - Completo: {arquivos['completo']}")
        logger.info(f" - Únicos: {arquivos['uniq']}")
        return arquivos
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
