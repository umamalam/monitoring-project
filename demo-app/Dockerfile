FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV APP_LOG_PATH=/var/log/app/app.log
RUN mkdir -p /var/log/app
EXPOSE 5000
CMD gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 2 --threads 4 app:app
