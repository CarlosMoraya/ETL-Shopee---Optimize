# ETL Shopee - Monitoramento de Motoristas

Pipeline ETL para extração de dados do sistema Shopee Logistics com carga em banco de dados Neon (PostgreSQL).

## 📊 Visão Geral

| Tela | Frequência | Status |
|------|------------|--------|
| Monitoramento de Motoristas | 15 em 15 min | ✅ Implementado |
| Tela 2 | 15 em 15 min | 🔲 Pendente |
| Tela 3 | Diário | 🔲 Pendente |
| Tela 4 | Diário | 🔲 Pendente |
| Tela 5 | Diário | 🔲 Pendente |

## 🏗️ Arquitetura

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐     ┌──────────────┐
│   Shopee    │────▶│  GitHub Actions │────▶│  Playwright │────▶│    Neon      │
│  Logistics  │     │   (Scheduler)   │     │  (Crawler)  │     │  (Postgres)  │
└─────────────┘     └─────────────────┘     └─────────────┘     └──────────────┘
```

## 🚀 Quick Start

### 1. Criar Banco no Neon

1. Acesse [neon.tech](https://neon.tech) e crie uma conta
2. Crie um novo projeto
3. Copie a **Connection String** (em **Connection Details**)

### 2. Configurar Secrets no GitHub

No seu repositório GitHub, vá em **Settings → Secrets and variables → Actions** e adicione:

| Secret | Descrição | Exemplo |
|--------|-----------|---------|
| `SHOPEE_EMAIL` | Email de login da Shopee | `carlos@empresa.com` |
| `SHOPEE_PWD` | Senha de login | `sua_senha` |
| `NEON_DATABASE_URL` | Connection string do Neon | `postgresql://user:pass@ep-xyz.us-east-2.aws.neon.tech/db?sslmode=require` |

### 3. Estrutura do Projeto

```
ETL - Shopee 02/
├── .github/workflows/
│   └── etl-shopee-monitoramento.yml   # Pipeline (15 min)
├── src/
│   ├── extractors/                    # Crawlers
│   ├── transformers/                  # Transformações
│   ├── loader/                        # Load para Neon
│   ├── pipelines/                     # Pipelines completos
│   └── utils/                         # Utilitários
├── data/
│   ├── raw/                           # Dados brutos (não versionar)
│   └── processed/                     # Dados processados
├── requirements.txt
├── .env.example
└── README.md
```

## 📦 Instalação Local (Desenvolvimento)

```bash
# Clonar repositório
git clone <seu-repo>
cd "ETL - Shopee 02"

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instalar dependências
pip install -r requirements.txt

# Instalar browsers do Playwright
playwright install chromium

# Copiar .env.example para .env
cp .env.example .env

# Editar .env com suas credenciais
```

## 🏃 Rodar Localmente

### Pipeline Completo

```bash
python -m src.pipelines.shopee_monitoramento_pipeline
```

### Apenas Extração

```bash
python -m src.extractors.shopee_monitoramento_crawler
```

## 📅 Agendamento (GitHub Actions)

O pipeline roda automaticamente **a cada 15 minutos** (minutos 0, 15, 30, 45 de cada hora).

Para rodar manualmente:
1. Vá em **Actions** no GitHub
2. Selecione **"ETL Shopee - Monitoramento (15 min)"**
3. Clique em **"Run workflow"**

## 🗄️ Schema do Banco

### Tabela: `shopee_monitoramento`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `driver_id` | VARCHAR | ID do motorista (extraído de `[ID] Nome`) |
| `driver_name` | VARCHAR | Nome do motorista |
| `assigned` | INTEGER | Total atribuído |
| `handed_over` | INTEGER | Total entregue |
| `delivered_qtd` | INTEGER | Quantidade entregue |
| `on_hold` | INTEGER | Quantidade em espera |
| `delivering_qtd` | INTEGER | Quantidade em entrega |
| `extracted_at` | TIMESTAMP | Data/hora da extração |

## 🔧 Transformações

Os dados passam pelas seguintes transformações:

1. **Separação do Driver ID**: Extrai o ID do formato `[ID] Nome`
2. **Normalização de colunas**: 
   - Minúsculas
   - Underscores no lugar de espaços
   - `(#)` → `_qtd`, `(%)` → `_perc`
3. **Timestamp**: Adiciona coluna `extracted_at`

## 🐛 Debug

### Logs do GitHub Actions

1. Vá em **Actions** → Selecione o workflow
2. Clique no job falho
3. Veja os logs de cada step

### Screenshots de Erro

Em caso de falha, o crawler gera:
- `erro_login.png` - Falha no login
- `erro_crawler.png` - Falha durante extração

### Rodar com Debug Local

```bash
# Rodar com interface (não headless)
export CRAWLER_HEADLESS=false
python -m src.extractors.shopee_monitoramento_crawler
```

## 📝 Próximos Passos

- [ ] Implementar Tela 2 (15 min)
- [ ] Implementar Telas 3-5 (Diário)
- [ ] Adicionar validações com Great Expectations
- [ ] Configurar alertas de falha (Slack/Email)

## 🔐 Segurança

- ⚠️ **Nunca** commitar o arquivo `.env`
- ✅ Usar **GitHub Secrets** para credenciais
- ✅ O `.gitignore` já ignora dados sensíveis

## 📞 Suporte

Em caso de dúvidas, consulte:
- [Docs do Playwright](https://playwright.dev/python/)
- [Docs do Neon](https://neon.tech/docs/)
- [GitHub Actions Docs](https://docs.github.com/en/actions)
