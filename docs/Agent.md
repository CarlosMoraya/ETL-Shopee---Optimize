# ETL Shopee Optimize - Agente Contextual

> Ponto de partida obrigatório para qualquer Agente de IA trabalhando neste projeto.

## 🎯 Regra de Ouro (Crucial)
**ESTE ARQUIVO DEVE SER ATUALIZADO A CADA ALTERAÇÃO ARQUITETURAL IMPORTANTE.**
Sempre que você criar um novo crawler, mudar um script de pipeline, ou alterar o schema do banco de dados Neon, você **DEVE** atualizar este arquivo.

## 🏗️ Arquitetura do Projeto
O projeto é um pipeline ETL para extração de dados da plataforma **Shopee Logistics**.

- **Extract**: Crawlers em Python usando **Playwright** para navegar no portal e baixar relatórios CSV/Excel.
- **Transform**: Limpeza e normalização de dados usando **Pandas**.
- **Load**: Carga dos dados processados em um banco de dados **Neon (PostgreSQL)**.
- **Infra**: Preparado para execução local ou via **GitHub Actions / Cloud Run Jobs**.

## 🚀 Tecnologias Core
- **Linguagem**: Python 3.13+
- **Browser Automation**: Playwright
- **Data Processing**: Pandas / Openpyxl
- **Database**: Neon (Postgres)
- **Environment**: Dotenv (.env para segredos locais)

## 🗂️ Estrutura de Diretórios
- `src/extractors/`: Scripts individuais de automação do portal (Crawlers).
- `src/pipelines/`: Orquestração completa (Extração -> Transformação -> Carga).
- `src/utils/`: Loggers, configuração de ambiente e conexões com DB.
- `data/raw/`: Arquivos baixados diretamente da Shopee (ignorados pelo git).
- `data/processed/`: CSVs normalizados prontos para carga.

## 🧭 Histórico de Marcos e Decisões
- **2026-04-09**: [FIX] Correção crítica no seletor do botão "Export AT" (atualizado para inglês e classe SSC) e implementação de polling robusto via Task Center direto, resolvendo falhas de extração e garantindo volume total (12k+ registros).
- **2026-04-09**: Execução bem-sucedida do `shopee_atribuicao_crawler.py` em modo **não-headless** para verificação humana.
- **2026-04-09**: [MARCO] Normalização de dados de atribuição com extração de `driver_id` de strings formatadas como `[ID] Nome`.
- **2026-03-XX**: Migração da arquitetura para usar **Neon (PostgreSQL)** em vez de BigQuery para maior flexibilidade e compatibilidade com o dashboard.

## 🤖 Regras de Comportamento do Agente IA
1. **Verificação de .env**: Sempre verifique se as credenciais `SHOPEE_EMAIL` e `SHOPEE_PWD` estão configuradas antes de sugerir execuções.
2. **Logs**: Todos os scripts devem usar o logger central em `src.utils.get_logger`.
3. **Cabeçalhos Normalizados**: Siga o padrão de colunas em minúsculas, sem caracteres especiais e usando underscores.
4. **Segurança**: Nunca exponha segredos em arquivos de log ou commits.
