FROM python:3.12-slim
WORKDIR /app

# Copiar y instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar script
COPY IngestaMongo.py .

CMD ["python", "IngestaMongo.py"]