# Predictive Infrastructure Monitoring

A monitoring pipeline for a multi-endpoint Flask service, built on Kafka and
Elasticsearch, with two detection layers and a forecasting layer:

- **Reactive detection** -- flags a metric that is abnormal right now
  (rolling z-score, plus an Isolation Forest model over the combined metric
  set for anomalies no single metric shows on its own)
- **Predictive forecasting** -- fits a trend to a metric's recent history
  and projects forward to estimate when it will cross a configured
  threshold, with a confidence interval and cross-metric corroboration
- **Accuracy tracking** -- checks past predictions against what actually
  happened, so prediction quality is a measured number, not a claim

The demo application includes two real, reproducible failure modes: an
unindexed query that slows down as data grows, and a connection pool that
slowly leaks under load and will eventually hit its configured limit. The
pipeline is built to catch both -- the first reactively, the second before
it happens.

## Architecture

```
Flask app (demo-app) --> Kafka (Upstash) --> pipeline worker --> Elasticsearch (Bonsai) --> Grafana
                                                   |
                                    ingest / detector / ml_detector
                                    / predictor / accuracy
```

The pipeline worker runs as a single Flask process with five background
threads (Render's free tier only supports Web Services, not Background
Workers):

| Thread | File | Role |
|---|---|---|
| Ingest | `ingest.py` | Consumes Kafka, writes raw logs, rolls request/gauge events into a unified `timeseries` index |
| Reactive detector | `detector.py` | Rolling z-score per metric |
| ML detector | `ml_detector.py` | Isolation Forest over each service's combined metrics |
| Predictor | `predictor.py` | Trend forecast + threshold crossing + confidence |
| Accuracy tracker | `accuracy.py` | Confirms or invalidates past predictions |

## Project structure

```
demo-app/
  app.py            Flask app: product listing, search, login, checkout
                     (with a real unindexed-scan slowdown and a real
                     connection leak), health check
pipeline/
  ingest.py          Kafka consumer, log/metric ingestion
  detector.py         Reactive z-score alerting
  ml_detector.py       Isolation Forest multivariate anomaly detection
  forecaster.py         Trend fitting (linear regression / Holt smoothing)
  correlate.py           Cross-metric corroboration
  predictor.py            Threshold-crossing predictions
  accuracy.py              Prediction outcome tracking
  es_client.py              Elasticsearch index management and queries
  kafka_client.py            Kafka producer/consumer config
  worker.py                   Flask app + background thread orchestration
  thresholds.json              Per-metric threshold configuration
  locustfile.py                  Load test traffic generator
docker-compose.local.yml    Local Kafka + Elasticsearch for development
```

## Tech stack

Python, Flask, Kafka (Upstash), Elasticsearch (Bonsai), Grafana Cloud,
scikit-learn, statsmodels, NumPy, Docker, Render, Locust.

## Local development

Requires Docker.

```bash
docker compose -f docker-compose.local.yml up -d
```

Terminal 1:
```bash
cd demo-app
pip install -r requirements.txt
export UPSTASH_KAFKA_BOOTSTRAP_SERVER=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT
export APP_LOG_PATH=/tmp/app.log
python3 app.py
```

Terminal 2:
```bash
cd pipeline
pip install -r requirements.txt
export UPSTASH_KAFKA_BOOTSTRAP_SERVER=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT
export BONSAI_URL=http://localhost:9200
python3 worker.py
```

Terminal 3 (load generator):
```bash
cd pipeline
pip install -r requirements-dev.txt
locust -f locustfile.py --host http://localhost:5000
```
Open `http://localhost:8089` and start a run with 40-50 users.

To see predictions appear faster during development, the demo app accepts:
`MAX_CONNECTIONS`, `CONNECTION_LEAK_RATE`, `CONNECTION_REPORT_INTERVAL_SECONDS`;
the worker accepts `DETECTOR_INTERVAL_SECONDS`, `PREDICTOR_INTERVAL_SECONDS`,
`ACCURACY_INTERVAL_SECONDS`, `WINDOW_SECONDS`.

## Deployment

### 1. Kafka -- Upstash

Create a free cluster at [console.upstash.com](https://console.upstash.com)
and a topic named `app-logs`. From the cluster page, note the bootstrap
endpoint, username, and password.

### 2. Elasticsearch -- Bonsai

Create a free Sandbox cluster at [bonsai.io](https://bonsai.io). The
cluster URL includes credentials.

### 3. Render

Deploy `demo-app` and `pipeline` as two separate Web Services from the same
repository (root directory set accordingly for each; both use their
included Dockerfile).

`demo-app` environment variables:
```
UPSTASH_KAFKA_BOOTSTRAP_SERVER
UPSTASH_KAFKA_USERNAME
UPSTASH_KAFKA_PASSWORD
```

`pipeline` environment variables:
```
UPSTASH_KAFKA_BOOTSTRAP_SERVER
UPSTASH_KAFKA_USERNAME
UPSTASH_KAFKA_PASSWORD
BONSAI_URL
```

Render's free tier spins services down after 15 minutes of inactivity;
[UptimeRobot](https://uptimerobot.com) pinging each service's `/health`
endpoint keeps them warm.

### 4. Grafana Cloud

Add an Elasticsearch data source pointed at the Bonsai URL, index
`timeseries`. Suggested panels:

| Panel | Index | Filter |
|---|---|---|
| Error rate by service | `timeseries` | `metric: error_rate` |
| Latency by service | `timeseries` | `metric: avg_latency_ms` |
| Active alerts | `alerts` | `resolved: false` |
| Predicted issues | `predictions` | `resolved: false`, sorted by `hours_until_threshold` |
| Connection trend | `timeseries` | `metric: active_connections` |

## API

| Endpoint | Description |
|---|---|
| `GET /health` | Service and thread status |
| `GET /alerts` | Active reactive alerts (`?source=zscore` or `?source=isolation_forest`) |
| `GET /predictions` | Active predictions, sorted by urgency |
| `GET /predictions/accuracy` | Aggregate prediction accuracy |

## How prediction works

`forecaster.py` fits ordinary least-squares regression to a metric's recent
history, upgrading to Holt's exponential smoothing once enough windows
exist (20+). The fit's residual spread produces a confidence interval on
the projected threshold-crossing time.

`correlate.py` checks whether other metrics for the same service are
trending in the same direction at the same time; agreement across metrics
raises the prediction's confidence score.

`predictor.py` combines both for every metric with a configured threshold
(`thresholds.json`) and writes a prediction with a threshold-crossing
estimate, confidence level, and supporting metrics.

`accuracy.py` checks predictions after their time window has passed and
records whether the metric actually crossed the threshold, and by how many
hours the estimate was off.

`ml_detector.py` standardizes every metric a service reports and fits an
Isolation Forest per detection cycle, flagging windows where the combined
metric state is unusual even if no single metric crosses its own z-score
threshold.

## Limitations

- Free-tier Kafka/Elasticsearch have message and storage caps.
- Render's free tier spins down without traffic; UptimeRobot is a
  workaround, not a fix.
- The z-score detector evaluates one metric at a time; `ml_detector.py`
  covers correlated multi-metric cases but is unsupervised, retrained from
  scratch each cycle, and not validated against labeled incident data.
- The predictive layer is trend extrapolation, not a learned model --
  it assumes the recent rate of change continues, and will miss sudden
  step-changes (a deploy that instantly doubles error rate, for example).
- Cross-metric correlation checks trend direction only, not statistical or
  causal correlation, and can be misled by metrics that happen to drift
  together for unrelated reasons.
- Confidence thresholds (`forecaster.py`'s spread-ratio cutoffs,
  `correlate.py`'s per-signal boost, `ml_detector.py`'s contamination
  parameter) are fixed constants, not tuned against real incident data.
- All five pipeline threads run in one process; a crash in one can take
  down the others.
- The demo app's connection pool is in-process memory and only behaves
  correctly with a single app instance (`gunicorn --workers 1`).
