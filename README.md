# Real-Time System Monitoring Pipeline

A real-time monitoring system that ingests application logs through a streaming pipeline, stores them for analysis, and detects anomalies using statistical baselining -- built to mirror how production observability stacks work at a small scale.

## Architecture

Flask App (real endpoints, real bugs)
        |  produces events
        v
   Apache Kafka (Redpanda Serverless)
        |  consumed by background worker
        v
   OpenSearch (log storage + metric aggregation)
        |
        v
   Anomaly Detector (rolling z-score on error rate and latency)
        |
        v
   Grafana (dashboards and visualization)

## What it does

- A Flask application exposes several endpoints (/products, /login, /checkout, /search) with realistic behavior: variable latency, and genuine failure modes on /checkout (timeouts and upstream errors), rather than randomly injected fake errors.
- Every request is logged as a structured JSON event and published directly to a Kafka topic.
- A worker service consumes the stream, indexes raw logs into OpenSearch, and rolls events up into one-minute metric windows per service (request count, error rate, average and p95 latency).
- An anomaly detector runs alongside the consumer, comparing each new window against a rolling baseline (mean and standard deviation of recent windows) and raising an alert when error rate or latency deviates significantly (z-score thresholding).
- A load generator simulates realistic traffic patterns for testing and demos.

## Tech stack

- Backend: Python, Flask
- Streaming: Apache Kafka (via Redpanda Serverless)
- Storage: OpenSearch (via Bonsai)
- Visualization: Grafana Cloud
- Deployment: Docker, Render

## Project structure

demo-app/
  app.py              - Flask app and Kafka producer
  load_generator.py   - Traffic simulator
  console_consumer.py - Lightweight terminal-based live stats viewer
  Dockerfile

pipeline/
  worker.py           - Kafka consumer and anomaly detector (runs as background threads)
  kafka_client.py
  es_client.py
  locustfile.py       - Load testing
  Dockerfile

## Running locally

1. Set up a Kafka topic and an OpenSearch index (Redpanda Serverless and Bonsai both have free tiers).

2. Start the app:

cd demo-app
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS="..."
export KAFKA_USERNAME="..."
export KAFKA_PASSWORD="..."
python3 app.py

3. Start the worker:

cd pipeline
pip install -r requirements.txt
export KAFKA_BOOTSTRAP_SERVERS="..."
export KAFKA_USERNAME="..."
export KAFKA_PASSWORD="..."
export BONSAI_URL="https://user:pass@your-cluster.bonsaisearch.net"
python3 worker.py

4. Generate traffic:

cd demo-app
python3 load_generator.py --duration 300

## Design notes

- The Kafka consumer and anomaly detector run as background threads inside a single Flask process rather than as separate services. This was a deliberate choice to fit within a free-tier hosting constraint, traded off against the fact that a crash in one thread can affect the other -- acceptable for this scale, not how it would be structured with a larger infrastructure budget.
- Anomaly detection uses a single-metric rolling z-score rather than a multivariate model. It is simple and explainable, at the cost of not catching correlated anomalies across multiple metrics -- a natural next step would be an Isolation Forest or autoencoder trained on multiple features jointly.
- Free-tier managed services (Kafka, OpenSearch) impose message and storage limits that are fine for demonstration traffic but would need upgrading for production volume.

## Possible extensions

- Multivariate anomaly detection (Isolation Forest or autoencoder)
- Root-cause correlation: surfacing the top error messages from the window that triggered an alert
- Alert routing to Slack or PagerDuty via webhook
- Swapping OpenSearch for a dedicated time-series store for the metrics path
