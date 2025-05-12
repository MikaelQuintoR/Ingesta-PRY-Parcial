FROM python:3.12-slim
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar datos y script
COPY data/ ./data/
COPY IngestaMySQL.py .

CMD ["python", "IngestaMySQL.py"]