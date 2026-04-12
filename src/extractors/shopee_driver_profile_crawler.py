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

            # 4. CLICAR NO BOTÃO "EXPORTAR" PARA ABRIR DROPDOWN
            logger.info("Clicando em 'Exportar' para abrir dropdown...")
            try:
                botao_exportar = page.locator('button:has-text("Exportar")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()
            except Exception:
                botao_exportar = page.locator('button:has-text("Export")').first
                await botao_exportar.wait_for(timeout=10_000)
                await botao_exportar.click()

            await page.wait_for_timeout(2_000)
            await page.screenshot(path=str(output_path / "dropdown_aberto.png"))

            # 5. CLICAR NA OPÇÃO 1 "EXPORTAR" DO DROPDOWN (NÃO "Histórico de exportações")
            logger.info("Clicando na opção 'Exportar' do dropdown (opção 1)...")
            
            # Estratégia 1: Tentar clicar diretamente usando keyboard (Tab + Enter)
            # Isso funciona mesmo com elementos ocultos
            try:
                logger.info("Tentando navegação por teclado...")
                await page.keyboard.press("Tab")
                await page.wait_for_timeout(500)
                await page.keyboard.press("Enter")
                logger.info("✅ Exportação solicitada via teclado!")
            except Exception as e:
                logger.warning(f"Teclado falhou: {e}")
                
                # Estratégia 2: Usar evaluate para clicar via JavaScript (ignora visibility)
                try:
                    logger.info("Tentando click via JavaScript...")
                    await page.evaluate("""
                        () => {
                            const items = document.querySelectorAll('.popover.ssc-tooltip-popover.searcher-with-history-dropdown li, .popover.ssc-tooltip-popover.searcher-with-history-dropdown [role="menuitem"], .popover.ssc-tooltip-popover.searcher-with-history-dropdown div');
                            for (let item of items) {
                                if (item.textContent.includes('Exportar') && !item.textContent.includes('Histórico')) {
                                    item.click();
                                    return true;
                                }
                            }
                            return false;
                        }
                    """)
                    logger.info("✅ Exportação solicitada via JavaScript!")
                except Exception as e2:
                    logger.warning(f"JavaScript falhou: {e2}")
                    
                    # Estratégia 3: Fallback - forçar click com force=True
                    try:
                        logger.info("Tentando force click...")
                        opcao = page.locator('text=Exportar').first
                        await opcao.click(force=True, timeout=10_000)
                        logger.info("✅ Exportação solicitada com force click!")
                    except Exception as e3:
                        logger.error(f"Todas as estratégias falharam: {e3}")
                        raise

            logger.info("Exportação solicitada — aguardando 90s para processamento do servidor...")
            await page.wait_for_timeout(90_000)
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

            # 7. ENCONTRAR E CLICAR NO BOTÃO "BAIXAR" DA TAREFA "Br Agency Assignment Task"
            logger.info("Procurando tarefa 'Br Agency Assignment Task' no painel...")
            caminho_arquivo = None
            botao_baixar = None
            encontrado = False
            
            # Aguardar painel carregar completamente
            await page.wait_for_timeout(5_000)
            
            for tentativa_baixar in range(6):
                try:
                    # Estratégia 1: Procurar pela tarefa específica "Br Agency Assignment" ou "Agency Assignment"
                    logger.info(f"Tentativa {tentativa_baixar + 1}: Procurando por 'Agency Assignment'...")
                    
                    # Encontrar o container da tarefa que contém "Agency Assignment"
                    # O painel tem múltiplas tarefas, precisamos encontrar a correta
                    tarefas = page.locator('[class*="task"], [class*="item"]').all()
                    
                    for tarefa in tarefas:
                        try:
                            texto_tarefa = await tarefa.text_content(timeout=2_000)
                            if texto_tarefa and "Agency Assignment" in texto_tarefa:
                                logger.info(f"✅ Tarefa 'Agency Assignment' encontrada!")
                                # Dentro desta tarefa, encontrar o botão "Baixar"
                                botao_baixar = tarefa.locator('button:has-text("Baixar"), button:has-text("Download")').first
                                await botao_baixar.wait_for(timeout=5_000)
                                encontrado = True
                                logger.info("✅ Botão 'Baixar' da tarefa correta encontrado!")
                                break
                        except:
                            continue
                    
                    if encontrado:
                        break
                    
                    # Estratégia 2: Se não encontrou por texto, reabrir o painel para atualizar
                    logger.info(f"Tarefa 'Agency Assignment' não encontrada — reabrindo painel...")
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(2_000)
                    icone = page.locator('div[data-v-13320df0].icon').first
                    await icone.wait_for(timeout=5_000)
                    await icone.click()
                    await page.wait_for_timeout(3_000)
                    await page.screenshot(path=str(output_path / f"painel_atualizado_{tentativa_baixar}.png"))
                    
                except Exception as e:
                    elapsed_extra = (tentativa_baixar + 1) * 30
                    logger.warning(f"Tentativa {tentativa_baixar + 1} falhou: {e}")
                    await page.screenshot(path=str(output_path / f"erro_tentativa_{tentativa_baixar}.png"))

            if not encontrado:
                # Fallback final: usar o primeiro botão "Baixar" mas com validação rigorosa depois
                logger.warning("⚠️ Não foi possível identificar tarefa específica, usando fallback...")
                botao_baixar = page.locator('button:has-text("Baixar"), button:has-text("Download")').first
                try:
                    await botao_baixar.wait_for(timeout=30_000)
                    encontrado = True
                except:
                    await page.screenshot(path=str(output_path / "erro_sem_baixar.png"))
                    raise Exception("Timeout: botão 'Baixar' não apareceu no painel após 180s adicionais.")

            # 8. DOWNLOAD — clica no botão "Baixar" da tarefa correta
            logger.info("Clicando em 'Baixar' na tarefa 'Agency Assignment'...")
            async with page.expect_download(timeout=120_000) as download_info:
                await botao_baixar.click()

            download = await download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_arquivo = output_path / f"shopee_driver_profile_{timestamp}_{download.suggested_filename}"
            await download.save_as(str(caminho_arquivo))
            logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")
            logger.info(f"Nome do arquivo sugerido: {download.suggested_filename}")

            # 9. VALIDAÇÃO: Verificar se o arquivo baixado é realmente de driver profile
            logger.info("Validando arquivo baixado...")
            sufixo_validacao = Path(caminho_arquivo).suffix.lower()
            df_validacao = None
            if sufixo_validacao == ".csv":
                df_validacao = pd.read_csv(caminho_arquivo, nrows=5)
            else:
                df_validacao = pd.read_excel(caminho_arquivo, nrows=5)

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
    sufixo = Path(caminho_arquivo).suffix.lower()
    if sufixo == ".csv":
        df = pd.read_csv(caminho_arquivo)
    else:
        df = pd.read_excel(caminho_arquivo)
    
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
