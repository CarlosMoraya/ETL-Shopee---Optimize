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
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"


def _ler_arquivo(caminho: Path, nrows=None):
    """Lê CSV, Excel ou ZIP (extraindo o primeiro CSV/Excel para disco antes de ler)."""
    import pandas as pd

    sufixo = caminho.suffix.lower()
    if sufixo == ".zip":
        with zipfile.ZipFile(caminho) as zf:
            nomes = zf.namelist()
            logger.info(f"Conteúdo do ZIP: {nomes}")
            # Prioriza CSV; se não houver, pega o primeiro Excel
            alvo = next((n for n in nomes if n.lower().endswith(".csv")), None) or \
                   next((n for n in nomes if n.lower().endswith((".xlsx", ".xls"))), None)
            if alvo is None:
                raise Exception(f"ZIP não contém CSV nem Excel. Arquivos: {nomes}")
            logger.info(f"Extraindo '{alvo}' do ZIP para disco...")
            # Extrai para arquivo temporário em disco — pd.read_excel falha com ZipExtFile
            with tempfile.TemporaryDirectory() as tmpdir:
                zf.extract(alvo, tmpdir)
                extraido = Path(tmpdir) / alvo
                if extraido.suffix.lower() == ".csv":
                    return pd.read_csv(extraido, nrows=nrows)
                return pd.read_excel(extraido, nrows=nrows)
    if sufixo == ".csv":
        return pd.read_csv(caminho, nrows=nrows)
    return pd.read_excel(caminho, nrows=nrows)
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

            # 3. CLICAR EM "PROCURAR"
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

            # 4+5. CLICAR EM "EXPORTAR" E ACIONAR O ITEM DO DROPDOWN
            logger.info("Clicando em 'Exportar' para abrir dropdown...")
            botao_exportar = page.locator('button:has-text("Exportar"), button:has-text("Export")').first
            await botao_exportar.wait_for(timeout=10_000)
            await botao_exportar.click()

            hora_antes_export = datetime.now()
            logger.info(f"🕐 Horário do clique em Exportar: {hora_antes_export.strftime('%Y-%m-%d %H:%M:%S')}")

            # Aguardar dropdown abrir e capturar screenshot diagnóstico
            await page.wait_for_timeout(1_000)
            await page.screenshot(path=str(output_path / "dropdown_aberto.png"))

            export_sucesso = False

            # Estratégia 1: Playwright locator nativo — suporta :has-text e force click
            logger.info("Tentativa 1: Localizando item do dropdown via Playwright...")
            for sel in [
                '.el-dropdown-menu__item:first-child',
                '.el-dropdown-menu li:first-child',
                'li.el-dropdown-menu__item',
                '[role="menuitem"]',
            ]:
                item = page.locator(sel).first
                cnt = await item.count()
                if cnt > 0:
                    txt = await item.inner_text()
                    logger.info(f"Item encontrado via '{sel}': '{txt}' — clicando...")
                    try:
                        await item.click(timeout=2_000)
                    except Exception:
                        await item.click(force=True)
                    logger.info("✅ Item de dropdown clicado!")
                    export_sucesso = True
                    break

            # Estratégia 2: Teclado (ArrowDown + Enter no botão Exportar)
            if not export_sucesso:
                logger.info("Tentativa 2: Teclado (ArrowDown + Enter)...")
                try:
                    await botao_exportar.click()
                    await page.wait_for_timeout(400)
                    await page.keyboard.press("ArrowDown")
                    await page.wait_for_timeout(200)
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(1_000)
                    export_sucesso = True
                    logger.info("✅ ArrowDown + Enter executado!")
                except Exception as e:
                    logger.warning(f"Teclado falhou: {e}")

            # Estratégia 3: JavaScript — APENAS em elementos de dropdown (não navegação)
            if not export_sucesso:
                logger.info("Tentativa 3: JavaScript no menu dropdown...")
                try:
                    await botao_exportar.click()
                    await page.wait_for_timeout(500)
                    result = await page.evaluate("""
                        () => {
                            // Busca APENAS em menus dropdown, não em navegação lateral
                            const menus = document.querySelectorAll(
                                '.el-dropdown-menu, [class*="dropdown-menu"]'
                            );
                            for (const menu of menus) {
                                const items = menu.querySelectorAll('li, .el-dropdown-menu__item');
                                for (const item of items) {
                                    const txt = item.textContent.trim();
                                    if (txt && !txt.toLowerCase().includes('hist')) {
                                        item.click();
                                        return { success: true, text: txt };
                                    }
                                }
                                if (items.length > 0) {
                                    items[0].click();
                                    return { success: true, text: items[0].textContent.trim() };
                                }
                            }
                            return { success: false, reason: 'Nenhum menu dropdown encontrado no DOM' };
                        }
                    """)
                    if result.get('success'):
                        logger.info(f"✅ JS dropdown: '{result.get('text')}'")
                        export_sucesso = True
                    else:
                        logger.warning(f"JS: {result.get('reason')}")
                except Exception as e:
                    logger.warning(f"JavaScript falhou: {e}")

            # Estratégia 4: Coordenadas relativas ao botão Exportar
            if not export_sucesso:
                logger.info("Tentativa 4: Click por coordenadas abaixo do botão...")
                try:
                    await botao_exportar.click()
                    await page.wait_for_timeout(500)
                    coords = await page.evaluate("""
                        () => {
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const btn = buttons.find(b => b.textContent.trim() === 'Exportar' || b.textContent.trim() === 'Export');
                            if (!btn) return null;
                            const rect = btn.getBoundingClientRect();
                            return { x: rect.left + rect.width / 2, y: rect.bottom + 20 };
                        }
                    """)
                    if coords:
                        logger.info(f"Clicando em ({coords['x']:.0f}, {coords['y']:.0f})...")
                        await page.mouse.click(coords['x'], coords['y'])
                        export_sucesso = True
                        logger.info("✅ Click por coordenadas!")
                    else:
                        logger.warning("Botão Exportar não encontrado via JS")
                except Exception as e:
                    logger.warning(f"Coordenadas falharam: {e}")

            if not export_sucesso:
                await page.screenshot(path=str(output_path / "erro_exportar.png"))
                raise Exception("Não foi possível acionar a exportação via dropdown")

            # Aguardar processamento da exportação
            logger.info(f"⏳ Exportação iniciada às {hora_antes_export.strftime('%H:%M:%S')}")
            logger.info("Aguardando 120s para processamento do servidor...")
            await page.wait_for_timeout(120_000)

            hora_depois_export = datetime.now()
            logger.info(f"✅ Processamento concluído às {hora_depois_export.strftime('%H:%M:%S')}")
            await page.screenshot(path=str(output_path / "apos_exportar.png"))

            # 6. ABRIR PAINEL "ÚLTIMA TAREFA" via ícone de tarefas no header
            logger.info("Abrindo painel 'Última tarefa' via ícone de tarefas...")
            painel_aberto = False
            for tentativa_painel in range(4):
                try:
                    # Tenta o ícone de tarefas (div com classe icon próximo ao sino)
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

            # 7. ENCONTRAR E CLICAR NO BOTÃO "BAIXAR" DA TAREFA "Spx Driver" MAIS RECENTE
            logger.info("Procurando tarefa 'Spx Driver' mais recente no painel...")
            logger.info(f"🕐 Exportação foi iniciada às: {hora_antes_export.strftime('%Y-%m-%d %H:%M:%S')}")
            caminho_arquivo = None
            encontrado = False

            # Aguardar painel carregar completamente
            await page.wait_for_timeout(5_000)

            for tentativa_baixar in range(6):
                try:
                    # Estratégia 1: Procurar pela tarefa "Spx Driver" mais recente (pelo horário)
                    logger.info(f"Tentativa {tentativa_baixar + 1}: Procurando por 'Spx Driver' mais recente...")

                    # Usar JavaScript para encontrar TODAS as tarefas Spx Driver e seus horários
                    tarefas_info = await page.evaluate("""
                        () => {
                            const tarefas = [];
                            document.querySelectorAll('.el-scrollbar__view > div, [class*="task"], [class*="item"]').forEach(el => {
                                const text = el.textContent || '';
                                if (text.includes('Spx Driver') || text.includes('spx_driver')) {
                                    // Extrair horário do texto (formato: YYYY-MM-DD HH:MM:SS)
                                    const timeMatch = text.match(/\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}/);
                                    const horario = timeMatch ? timeMatch[0] : 'desconhecido';
                                    
                                    const buttons = el.querySelectorAll('button');
                                    buttons.forEach(btn => {
                                        if (btn.textContent.includes('Baixar') || btn.textContent.includes('Download')) {
                                            tarefas.push({
                                                text: text.substring(0, 200),
                                                horario: horario,
                                                buttonIndex: Array.from(el.querySelectorAll('button')).indexOf(btn)
                                            });
                                        }
                                    });
                                }
                            });
                            return tarefas;
                        }
                    """)
                    
                    logger.info(f"📋 Tarefas Spx Driver encontradas: {len(tarefas_info)}")
                    for i, tarefa in enumerate(tarefas_info):
                        logger.info(f"   {i+1}. Horário: {tarefa['horario']} - {tarefa['text'][:100]}")

                    if len(tarefas_info) > 0:
                        # Encontrar a tarefa MAIS RECENTE (maior horário)
                        tarefa_mais_recente = max(tarefas_info, key=lambda x: x['horario'])
                        logger.info(f"✅ Tarefa mais recente: {tarefa_mais_recente['horario']}")

                        # Verificar se o horário é POSTERIOR ao início da exportação
                        horario_tarefa = tarefa_mais_recente['horario']
                        tarefa_valida = True
                        if horario_tarefa != 'desconhecido':
                            from datetime import datetime as dt
                            try:
                                hora_tarefa_dt = dt.strptime(horario_tarefa, '%Y-%m-%d %H:%M:%S')
                                if hora_tarefa_dt >= hora_antes_export:
                                    logger.info(f"✅ Tarefa {horario_tarefa} é posterior à exportação — baixando!")
                                else:
                                    logger.warning(
                                        f"⚠️ Tarefa {horario_tarefa} é ANTERIOR à exportação "
                                        f"({hora_antes_export.strftime('%Y-%m-%d %H:%M:%S')}). "
                                        f"Aguardando nova tarefa..."
                                    )
                                    tarefa_valida = False
                            except Exception:
                                logger.warning("⚠️ Não conseguiu comparar horários — prosseguindo")

                        if not tarefa_valida:
                            # Pula download desta iteração; loop reabre painel e tenta novamente
                            pass
                        else:
                            # Clicar no botão "Baixar" da tarefa mais recente via JavaScript
                            async with page.expect_download(timeout=60_000) as download_info:
                                click_result = await page.evaluate("""
                                    () => {
                                        const tarefas = [];
                                        document.querySelectorAll('.el-scrollbar__view > div, [class*="task"], [class*="item"]').forEach(el => {
                                            const text = el.textContent || '';
                                            if (text.includes('Spx Driver') || text.includes('spx_driver')) {
                                                const buttons = el.querySelectorAll('button');
                                                buttons.forEach(btn => {
                                                    if (btn.textContent.includes('Baixar') || btn.textContent.includes('Download')) {
                                                        btn.click();
                                                        tarefas.push({ success: true });
                                                    }
                                                });
                                            }
                                        });
                                        return tarefas.length > 0 ? tarefas[0] : { success: false };
                                    }
                                """)

                            if click_result.get('success'):
                                logger.info("✅ Botão 'Baixar' clicado!")
                                download = await download_info.value
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                caminho_arquivo = output_path / f"shopee_driver_profile_{timestamp}_{download.suggested_filename}"
                                await download.save_as(str(caminho_arquivo))
                                logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")
                                logger.info(f"Nome do arquivo sugerido: {download.suggested_filename}")
                                encontrado = True
                                break
                            else:
                                logger.warning("⚠️ Encontrou tarefa mas não conseguiu clicar")
                    else:
                        logger.warning("⚠️ Nenhuma tarefa Spx Driver encontrada")

                    # Estratégia 2: Se não encontrou, reabrir o painel para atualizar
                    logger.info(f"Reabrindo painel para atualizar...")
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(2_000)
                    icone = page.locator('div[data-v-13320df0].icon').first
                    await icone.wait_for(timeout=5_000)
                    await icone.click()
                    await page.wait_for_timeout(3_000)
                    await page.screenshot(path=str(output_path / f"painel_atualizado_{tentativa_baixar}.png"))

                except Exception as e:
                    logger.warning(f"Tentativa {tentativa_baixar + 1} falhou: {e}")
                    await page.screenshot(path=str(output_path / f"erro_tentativa_{tentativa_baixar}.png"))

            if not encontrado:
                # Fallback final: clicar no PRIMEIRO botão "Baixar" via JavaScript
                logger.warning("⚠️ Não encontrou 'Spx Driver', clicando no primeiro botão 'Baixar'...")
                async with page.expect_download(timeout=60_000) as download_info:
                    click_result = await page.evaluate("""
                        () => {
                            const buttons = document.querySelectorAll('button');
                            for (let btn of buttons) {
                                if (btn.textContent.includes('Baixar') || btn.textContent.includes('Download')) {
                                    btn.click();
                                    return { success: true };
                                }
                            }
                            return { success: false };
                        }
                    """)
                    download = await download_info.value
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                caminho_arquivo = output_path / f"shopee_driver_profile_{timestamp}_{download.suggested_filename}"
                await download.save_as(str(caminho_arquivo))
                logger.info(f"✅ Arquivo baixado (fallback): {caminho_arquivo}")
                logger.info(f"Nome do arquivo sugerido: {download.suggested_filename}")

            # 9. VALIDAÇÃO: Verificar se o arquivo baixado é realmente de driver profile
            logger.info("Validando arquivo baixado...")
            df_validacao = _ler_arquivo(Path(caminho_arquivo), nrows=5)

            colunas_lower = [c.lower() for c in df_validacao.columns]
            colunas_texto = " ".join(colunas_lower)

            # Driver profile deve ter colunas específicas como "motorista", "driver", "cnh", "vehicle"
            indicadores_driver = ["motorista", "driver", "cnh", "veículo", "vehicle", "placa", "license"]
            tem_indicador_driver = any(ind in colunas_texto for ind in indicadores_driver)

            # PNR tickets tem colunas como "ticket", "pnr", "order", "assignee"
            indicadores_pnr = ["ticket", "pnr_order", "rejection_reason", "assignee"]
            tem_indicador_pnr = any(ind in colunas_texto for ind in indicadores_pnr)

            if tem_indicador_pnr and not tem_indicador_driver:
                logger.error("❌ VALIDAÇÃO FALHOU: Arquivo baixado parece ser de PNR Tickets, não de Driver Profile!")
                logger.error(f"Colunas encontradas: {df_validacao.columns.tolist()}")
                await page.screenshot(path=str(output_path / "erro_arquivo_incorreto.png"))
                raise Exception(
                    "Arquivo incorreto baixado! O painel retornou dados de PNR Tickets ao invés de Driver Profile. "
                    "Isso pode indicar que há uma exportação de PNR mais recente no painel."
                )

            if not tem_indicador_driver and not tem_indicador_pnr:
                logger.warning("⚠️ VALIDAÇÃO: Nenhuma coluna típica identificada. Verificando tamanho do arquivo...")
                if len(df_validacao) == 0:
                    raise Exception("Arquivo baixado está vazio!")

            logger.info("✅ Validação do arquivo concluída - arquivo parece ser de Driver Profile")

        finally:
            await browser.close()

    # 10. PROCESSAR COM PANDAS (arquivo já foi lido na validação, mas vamos recarregar completo)
    logger.info("Processando arquivo...")
    df = _ler_arquivo(Path(caminho_arquivo))
    
    logger.info(f"✅ Arquivo de Driver Profile confirmado com {len(df)} linhas")

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

    # Adicionar a nova coluna 'embarcador' na primeira posição
    df.insert(0, "embarcador", "Shopee_Last_Mile")

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
