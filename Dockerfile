# Use uma imagem base leve do Python
FROM python:3.10-slim

# Atualizar e instalar dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Definir o diretório de trabalho
WORKDIR /app

# Copiar apenas o arquivo de dependências
COPY requirements.txt .

# Instalar dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código da aplicação
COPY . .

# Configurar variáveis de ambiente (o arquivo de credenciais será montado pelo Secret Manager)
ENV GOOGLE_APPLICATION_CREDENTIALS="/etc/gcp-key.json"

# Expor a porta do servidor (caso necessário)
EXPOSE 11190

# Comando para iniciar o servidor
CMD ["python", "app.py"]
