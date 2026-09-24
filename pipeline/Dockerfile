FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY kafka_client.py es_client.py ingest.py detector.py ml_detector.py forecaster.py correlate.py predictor.py accuracy.py worker.py thresholds.json .
EXPOSE 5001
CMD gunicorn --bind 0.0.0.0:${PORT:-5001} --workers 1 --threads 4 --timeout 120 worker:app
