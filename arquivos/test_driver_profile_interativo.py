"""
Script de teste INTERATIVO para debug visual do crawler de Driver Profile
Roda localmente com navegador VISÍVEL para acompanhar a execução passo a passo
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


async def test_driver_profile_interativo():
    """Testa o crawler de Driver Profile com navegador VISÍVEL para acompanhar"""
    
    email = os.environ.get("SHOPEE_EMAIL", "")
    senha = os.environ.get("SHOPEE_PWD", "")

    if not email or not senha:
        raise Exception("SHOPEE_EMAIL e SHOPEE_PWD devem estar definidos.")

    output_path = DATA_RAW_DIR / "shopee_driver_profile" / "teste_interativo"
    output_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 80)
    logger.info("TESTE INTERATIVO: Shopee Perfil do Motorista (NAVEGADOR VISÍVEL)")
    logger.info("O navegador será aberto para você acompanhar cada passo!")
    logger.info("=" * 80)

    async with async_playwright() as p:
        logger.info("Iniciando navegador VISÍVEL...")
        browser = await p.chromium.launch(
            headless=False,  # ← MODO VISUAL!
            args=["--no-sandbox", "--disable-dev-shm-usage", "--start-maximized"],
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

        # Habilitar console logs do browser
        page.on("console", lambda msg: print(f"[BROWSER] {msg.text}"))

        try:
            # Aguardar usuário se preparar
            logger.info("\n⏳ O navegador será aberto em 5 segundos...")
            await asyncio.sleep(5)
            logger.info("\n🚀 Iniciando teste...\n")

            # 1. LOGIN
            logger.info(f"\n📌 [PASSO 1/9] Acessando portal: {PORTAL_URL}")
            logger.info("👁️  OBSERVE: Navegador deve abrir e carregar o portal")
            await page.goto(PORTAL_URL, wait_until="networkidle", timeout=60_000)
            logger.info("   ✅ Portal carregado")
            await asyncio.sleep(3)

            logger.info("\n📌 [PASSO 2/9] Aguardando formulário de login...")
            await page.locator('input[type="password"]').wait_for(timeout=30_000)
            logger.info("   ✅ Formulário encontrado")
            await asyncio.sleep(2)

            logger.info(f"\n📌 [PASSO 3/9] Preenchendo credenciais (email: {email})...")
            logger.info("👁️  OBSERVE: Campos de login sendo preenchidos")
            await page.locator('input[autocomplete="email"]').fill(email)
            await page.locator('input[type="password"]').fill(senha)
            logger.info("   ✅ Credenciais preenchidas")
            await asyncio.sleep(2)

            logger.info("\n📌 [PASSO 4/9] Submetendo login...")
            logger.info("👁️  OBSERVE: Login sendo submetido")
            await page.locator('input[type="password"]').press("Enter")
            logger.info("   ⏳ Aguardando login carregar...")
            
            try:
                await page.locator('text="Força de trabalho"').wait_for(timeout=30_000)
                logger.info("   ✅ Login confirmado!")
            except Exception:
                screenshot_path = output_path / "teste_login_erro.png"
                await page.screenshot(path=str(screenshot_path))
                logger.error(f"   ❌ Login falhou! Screenshot salvo em: {screenshot_path}")
                raise Exception("Login falhou")
            
            await asyncio.sleep(3)

            # 2. NAVEGAR PARA PERFIL DO MOTORISTA
            logger.info(f"\n📌 [PASSO 5/9] Navegando para: {DRIVER_PROFILE_URL}")
            logger.info("👁️  OBSERVE: Navegando para página de Driver Profile")
            await page.goto(DRIVER_PROFILE_URL, wait_until="domcontentloaded", timeout=60_000)
            await asyncio.sleep(5)
            logger.info("   ✅ Página de Driver Profile carregada")

            # 3. CLICAR EM "PROCURAR"
            logger.info("\n📌 [PASSO 6/9] Aguardando tabela carregar...")
            await page.wait_for_selector(".ssc-react-pro-table-table", timeout=60_000)
            logger.info("   ✅ Tabela carregada")

            logger.info("\n📌 [PASSO 6b/9] Clicando em 'Procurar'...")
            logger.info("👁️  OBSERVE: Clicando no botão 'Procurar'")
            try:
                botao_procurar = page.locator('button:has-text("Procurar")').first
                await botao_procurar.wait_for(timeout=20_000)
                await botao_procurar.click()
                logger.info("   ✅ 'Procurar' clicado")
                await asyncio.sleep(5)
            except Exception as e:
                logger.warning(f"   ⚠️ 'Procurar' não encontrado: {e}")

            # 4. ABRIR DROPDOWN DE EXPORTAR
            logger.info("\n📌 [PASSO 7/9] Abrindo dropdown 'Exportar'...")
            logger.info("👁️  OBSERVE: Clicando em 'Exportar' para abrir dropdown")
            botao_exportar = page.locator('button:has-text("Exportar")').first
            await botao_exportar.wait_for(timeout=10_000)
            await botao_exportar.click()
            logger.info("   ✅ Botão 'Exportar' clicado")
            await asyncio.sleep(2)
            await page.screenshot(path=str(output_path / "dropdown_aberto.png"))
            logger.info("   📸 Screenshot salvo: dropdown_aberto.png")

            # 5. CLICAR EM "EXPORTAR" (opção 1) via TECLADO (ArrowDown + Enter)
            logger.info("\n📌 [PASSO 7b/9] Clicando em 'Exportar' do dropdown (via ArrowDown + Enter)...")
            logger.info("👁️  OBSERVE: Pressionando ArrowDown (seleciona 1º item) + Enter (clica)")
            await asyncio.sleep(1)
            
            # Registrar horário
            hora_antes_export = datetime.now()
            logger.info(f"🕐 Horário antes do click: {hora_antes_export.strftime('%H:%M:%S')}")
            
            # Estratégia 1: ArrowDown + Enter
            await page.keyboard.press("ArrowDown")
            logger.info("   ⬇️ ArrowDown pressionado (seleciona 'Exportar')")
            await asyncio.sleep(0.5)
            await page.keyboard.press("Enter")
            logger.info("   ⏎ Enter pressionado")
            logger.info("   ✅ Exportação solicitada via teclado!")
            
            await asyncio.sleep(2)

            # 6. AGUARDAR PROCESSAMENTO
            logger.info("\n📌 [PASSO 8/9] Aguardando 30s para processamento do servidor...")
            logger.info(f"⏳ Exportação iniciada às {hora_antes_export.strftime('%H:%M:%S')}")
            logger.info("   💡 Você pode observar o navegador durante este tempo")
            logger.info("   ⏳ Aguardando...")
            
            # Dividir em blocos de 10s
            for i in range(3):
                await asyncio.sleep(10)
                logger.info(f"   ⏳ {10 * (i + 1)}s / 30s...")
            
            hora_depois_export = datetime.now()
            logger.info(f"✅ Processamento concluído às {hora_depois_export.strftime('%H:%M:%S')}")
            await page.screenshot(path=str(output_path / "apos_processamento.png"))

            # 7. ABRIR PAINEL "ÚLTIMA TAREFA"
            logger.info("\n📌 [PASSO 9/9] Abrindo painel 'Última tarefa'...")
            logger.info("👁️  OBSERVE: Abrindo painel e buscando Spx Driver mais recente")
            logger.info(f"🕐 Exportação foi às {hora_antes_export.strftime('%H:%M:%S')} - tarefa deve ser posterior!")
            
            try:
                icone = page.locator('div[data-v-13320df0].icon').first
                await icone.wait_for(timeout=10_000)
                await icone.click()
                logger.info("   ✅ Painel aberto")
                await asyncio.sleep(3)
                await page.screenshot(path=str(output_path / "painel_aberto.png"))
                
                # Buscar e mostrar TODAS as tarefas Spx Driver com horários
                logger.info("\n🔍 Procurando tarefas 'Spx Driver'...")
                
                tarefas_info = await page.evaluate("""
                    () => {
                        const tarefas = [];
                        document.querySelectorAll('.el-scrollbar__view > div, [class*="task"], [class*="item"]').forEach(el => {
                            const text = el.textContent || '';
                            if (text.includes('Spx Driver') || text.includes('spx_driver')) {
                                const timeMatch = text.match(/\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}/);
                                const horario = timeMatch ? timeMatch[0] : 'desconhecido';
                                tarefas.push({ text: text.substring(0, 150), horario: horario });
                            }
                        });
                        return tarefas;
                    }
                """)
                
                logger.info(f"📋 Tarefas Spx Driver encontradas: {len(tarefas_info)}")
                for i, tarefa in enumerate(tarefas_info):
                    logger.info(f"   {i+1}. Horário: {tarefa['horario']}")
                    logger.info(f"      {tarefa['text'][:100]}...")
                
                if len(tarefas_info) > 0:
                    # Clicar na mais recente
                    logger.info("\n👁️  OBSERVE: Clicando no botão 'Baixar' da tarefa mais recente...")
                    
                    await page.evaluate("""
                        () => {
                            document.querySelectorAll('.el-scrollbar__view > div, [class*="task"], [class*="item"]').forEach(el => {
                                const text = el.textContent || '';
                                if (text.includes('Spx Driver') || text.includes('spx_driver')) {
                                    const buttons = el.querySelectorAll('button');
                                    buttons.forEach(btn => {
                                        if (btn.textContent.includes('Baixar') || btn.textContent.includes('Download')) {
                                            btn.click();
                                        }
                                    });
                                }
                            });
                        }
                    """)
                    logger.info("   ✅ Botão 'Baixar' clicado!")
                else:
                    logger.warning("   ⚠️ Nenhuma tarefa Spx Driver encontrada")
                
            except Exception as e:
                logger.warning(f"   ⚠️ Erro ao abrir painel: {e}")

            logger.info("\n" + "=" * 80)
            logger.info("✅ TESTE INTERATIVO CONCLUÍDO!")
            logger.info(f"Screenshots salvos em: {output_path}")
            logger.info(f"Exportação: {hora_antes_export.strftime('%H:%M:%S')} → Download: {hora_depois_export.strftime('%H:%M:%S')}")
            logger.info("Verifique se a tarefa baixada tem horário POSTERIOR ao da exportação!")
            logger.info("=" * 80)

            # Manter navegador aberto para inspeção manual
            logger.info("\n👁️  O navegador permanecerá aberto por 60s para inspeção manual...")
            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"\n❌ ERRO NO TESTE: {e}")
            import traceback
            traceback.print_exc()
            await page.screenshot(path=str(output_path / "teste_erro_final.png"))
            input("\n👁️  Erro ocorrido. Pressione ENTER para fechar o navegador...")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(test_driver_profile_interativo())
