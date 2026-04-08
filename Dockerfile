# Usa a imagem oficial do Playwright compatível com a versão (1.49.1) presente no requirements.txt
FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

# Configurar variáveis de ambiente do Python 
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# O Docker do Playwright roda como usuário "pwuser" por motivos de segurança
WORKDIR /app

# Copiar arquivos de dependências
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo o código-fonte
COPY . .

# Comando Padrão de execução.
# O Cloud Run Jobs irá sobreescrever se precisarmos rodar pipelines específicos, 
# mas esse é um entrypoint útil para iniciar.
CMD ["python", "-m", "src.pipelines.shopee_atribuicao_pipeline"]
