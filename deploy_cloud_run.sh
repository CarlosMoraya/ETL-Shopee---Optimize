#!/bin/bash
set -e

# Configurações GCP
PROJECT_ID=$(gcloud config get-value project)
REGION="southamerica-east1"
JOB_NAME="shopee-atribuicao-etl"
IMAGE_NAME="gcr.io/$PROJECT_ID/$JOB_NAME"

echo "================================================="
echo "Preparando deploy para Cloud Run Jobs..."
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Image: $IMAGE_NAME"
echo "================================================="

# Certifique-se que o usuário configurou o NEON_DATABASE_URL e os Secrets da Shopee
if [[ -z "$SHOPEE_EMAIL" || -z "$SHOPEE_PWD" || -z "$NEON_DATABASE_URL" ]]; then
  echo "⚠️ AVISO: Configure as variáveis de ambiente no Cloud Run via console após o deploy ou adicione as flags '--set-env-vars' / '--set-secrets' abaixo."
fi

echo "1. Fazendo build da imagem e enviando para o Google Container Registry..."
gcloud builds submit --tag $IMAGE_NAME

echo "2. Criando/Atualizando o Cloud Run Job..."
gcloud run jobs update $JOB_NAME \
    --image $IMAGE_NAME \
    --region $REGION \
    --tasks 1 \
    --max-retries 1 \
    --task-timeout 30m \
    --memory 2048Mi \
    --cpu 1 \
    --set-env-vars "CRAWLER_HEADLESS=true" \
    --execute-now

echo "================================================="
echo "✅ Deploy efetuado e executado! Acompanhe nos logs do Google Cloud Console."
echo "Para agendar execuções diárias via Cloud Scheduler:"
echo "gcloud scheduler jobs create http agendamento-shopee-atribuicao \\"
echo "  --schedule=\"0 6 * * *\" \\"
echo "  --uri=\"https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$JOB_NAME:run\" \\"
echo "  --http-method=POST \\"
echo "  --oauth-service-account-email=\"<SUA_SERVICE_ACCOUNT>\""
echo "================================================="
