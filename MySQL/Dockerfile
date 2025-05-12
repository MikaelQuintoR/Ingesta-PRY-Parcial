FROM python:3.12-slim
WORKDIR /app

# Copiar dependencias y script\COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data/ ./data/
COPY IngestaMySQL.py .

CMD ["python", "IngestaMySQL.py"]