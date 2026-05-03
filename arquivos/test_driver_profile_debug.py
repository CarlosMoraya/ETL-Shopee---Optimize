"""
Script de teste para debug do crawler de Driver Profile
Gera screenshots detalhados em cada etapa para análise
"""
import asyncio
import os
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright

# Carregar .env
from dotenv import load_dotenv
load_dotenv()

from src.utils import get_logger, DATA_RAW_DIR

logger = get_logger(__name__)

PORTAL_URL = "https://logistics.myagencyservice.com.br/"
DRIVER_PROFILE_URL = "https://logistics.myagencyservice.com.br/#/workforce/driver-profile/list"


async def test_driver_profile_crawler():
    """Testa o crawler com screenshots detalhados"""
    
    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos.")

    output_path = DATA_RAW_DIR / "shopee_driver_profile" / "debug_test"
    output_path.mkdir(parents=True, exist_ok=True)
    
    step = 0
    async def screenshot(name):
        nonlocal step
        step += 1
        path = output_path / f"step_{step:02d}_{name}.png"
        try:
            await page.screenshot(path=str(path), full_page=True)
            logger.info(f"📸 Screenshot salvo: {path.name}")
        except Exception as e:
            logger.warning(f"⚠️ Falha ao salvar screenshot: {e}")

    logger.info("=" * 80)
    logger.info("TESTE: Shopee Perfil do Motorista (MODO DEBUG COM SCREENSHOTS)")
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
            logger.info(f"\n📌 [1] Acessando portal: {PORTAL_URL}")
            await page.goto(PORTAL_URL, wait_until="networkidle", timeout=60_000)
            await screenshot("portal_carregado")

            logger.info("📌 [2] Aguardando formulário de login...")
            await page.locator('input[type="password"]').wait_for(timeout=30_000)
            await screenshot("formulario_login")

            logger.info(f"📌 [3] Preenchendo credenciais...")
            await page.locator('input[autocomplete="email"]').fill(email)
            await page.locator('input[type="password"]').fill(senha)
            await screenshot("credenciais_preenchidas")

            logger.info("📌 [4] Submetendo login...")
            await page.locator('input[type="password"]').press("Enter")
            await page.wait_for_timeout(5_000)
            await screenshot("apos_login")

            # Verificar login
            try:
                await page.locator('text="Força de trabalho"').wait_for(timeout=30_000)
                logger.info("✅ Login confirmado!")
            except Exception:
                await screenshot("login_falhou")
                raise Exception("Login falhou")

            # 2. NAVEGAR PARA PERFIL DO MOTORISTA
            logger.info(f"\n📌 [5] Navegando para: {DRIVER_PROFILE_URL}")
            await page.goto(DRIVER_PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(5_000)
            await screenshot("pagina_driver_profile")

            # 3. CLICAR EM "PROCURAR"
            logger.info("\n📌 [6] Aguardando tabela carregar...")
            await page.wait_for_selector(".ssc-react-pro-table-table", timeout=60_000)
            await screenshot("tabela_carregada")

            logger.info("📌 [7] Clicando em 'Procurar'...")
            try:
                botao_procurar = page.locator('button:has-text("Procurar")').first
                await botao_procurar.wait_for(timeout=20_000)
                await botao_procurar.click()
                await page.wait_for_timeout(5_000)
                logger.info("✅ 'Procurar' clicado")
            except Exception as e:
                logger.warning(f"⚠️ 'Procurar' não encontrado: {e}")
            await screenshot("apos_procurar")

            # 4. ABRIR DROPDOWN DE EXPORTAR
            logger.info("\n📌 [8] Clicando em 'Exportar'...")
            botao_exportar = page.locator('button:has-text("Exportar")').first
            await botao_exportar.wait_for(timeout=10_000)
            await botao_exportar.click()
            await page.wait_for_timeout(2_000)
            await screenshot("dropdown_aberto")

            # 5. ANALISAR DROPDOWN
            logger.info("\n📌 [9] Analisando estrutura do dropdown...")
            dropdown_info = await page.evaluate("""
                () => {
                    const popover = document.querySelector('.popover.ssc-tooltip-popover.searcher-with-history-dropdown');
                    if (!popover) {
                        return { exists: false, allPopovers: [] };
                    }
                    
                    const items = Array.from(popover.querySelectorAll('li, [role="menuitem"], div'));
                    return {
                        exists: true,
                        visible: popover.offsetParent !== null,
                        classes: popover.className,
                        items: items.map(item => ({
                            tag: item.tagName,
                            classes: item.className,
                            text: item.textContent.trim().substring(0, 100),
                            visible: item.offsetParent !== null
                        }))
                    };
                }
            """)
            logger.info(f"📋 Dropdown info: {dropdown_info}")

            # 6. CLICAR EM "EXPORTAR" (opção 1)
            logger.info("\n📌 [10] Tentando clicar em 'Exportar' via JavaScript...")
            click_result = await page.evaluate("""
                () => {
                    const popover = document.querySelector('.popover.ssc-tooltip-popover.searcher-with-history-dropdown');
                    if (!popover) return { success: false, error: 'Popover não encontrado' };
                    
                    const items = popover.querySelectorAll('li, [role="menuitem"], div');
                    for (let item of items) {
                        const text = item.textContent.trim();
                        if (text === 'Exportar' || (text.includes('Exportar') && !text.includes('Histórico') && !text.includes('Exportação'))) {
                            item.click();
                            return { success: true, clicked: text };
                        }
                    }
                    return { success: false, error: 'Item não encontrado' };
                }
            """)
            logger.info(f"✅ Resultado do click: {click_result}")
            await page.wait_for_timeout(2_000)
            await screenshot("apos_click_exportar")

            # 7. AGUARDAR PROCESSAMENTO
            logger.info("\n📌 [11] Aguardando 30s para processamento...")
            await page.wait_for_timeout(30_000)
            await screenshot("pos_processamento")

            # 8. ABRIR PAINEL "ÚLTIMA TAREFA"
            logger.info("\n📌 [12] Abrindo painel 'Última tarefa'...")
            try:
                icone = page.locator('div[data-v-13320df0].icon').first
                await icone.wait_for(timeout=10_000)
                await icone.click()
                await page.wait_for_timeout(3_000)
                await screenshot("painel_aberto")
                
                # Analisar tarefas
                tarefas_info = await page.evaluate("""
                    () => {
                        const tasks = [];
                        document.querySelectorAll('[class*="task"], [class*="item"]').forEach(el => {
                            tasks.push({
                                text: el.textContent.trim().substring(0, 200),
                                hasDownload: el.textContent.includes('Baixar') || el.textContent.includes('Download')
                            });
                        });
                        return tasks;
                    }
                """)
                logger.info(f"📋 Tarefas encontradas: {tarefas_info}")
                
            except Exception as e:
                logger.warning(f"⚠️ Erro ao abrir painel: {e}")
                await screenshot("erro_painel")

            logger.info("\n" + "=" * 80)
            logger.info("✅ TESTE CONCLUÍDO!")
            logger.info(f"Verifique os {step} screenshots em: {output_path}")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"\n❌ ERRO: {e}")
            import traceback
            traceback.print_exc()
            await screenshot("erro_final")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(test_driver_profile_crawler())
