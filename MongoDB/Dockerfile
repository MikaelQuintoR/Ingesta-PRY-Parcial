FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY data.json /app/data.json
COPY IngestaMongo.py .
CMD ["python", "IngestaMongo.py"]