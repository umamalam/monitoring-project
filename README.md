# Real-Time System Monitoring — Cloud Edition (Kafka + Elasticsearch + Grafana)

A real multi-service app, generating real logs under real load (via Locust),
flowing through a real Kafka + Elasticsearch pipeline, with anomaly detection
and a Grafana dashboard — deployed entirely on free tiers.

## Why this architecture (for your interview)

- **Kafka (Upstash)** instead of Redis: industry-standard event streaming,
  free managed tier means no ops overhead for a demo.
- **Elasticsearch (Bonsai)** instead of SQLite: the standard log-storage
  choice at most companies (ELK stack), free tier is enough for demo volume.
- **Grafana Cloud** instead of a custom dashboard: what real SRE/DevOps teams
  actually stare at during incidents — free tier, no self-hosting.
- **Render**, not AWS EC2: git-push deploys, no manual SSH/Docker setup, free
  web services fit this project's scale.
- **Single process with background threads** for the pipeline worker: Render's
  free tier only offers Web Services (not Background Workers), so the Kafka
  consumer and anomaly detector run as threads inside one Flask app that also
  exposes a `/health` endpoint. This is a genuine, explainable trade-off —
  not a hack you need to hide in an interview.

---

## Part 1 — Sign up for the 3 managed free tiers

### 1. Upstash Kafka (free serverless Kafka)
1. Go to [console.upstash.com](https://console.upstash.com) → sign up (free).
2. Create a Kafka cluster → choose a region close to where Render will run
   (e.g. US East).
3. Create a topic named `app-logs`.
4. From the cluster details page, copy:
   - **Bootstrap Endpoint** → `UPSTASH_KAFKA_BOOTSTRAP_SERVER`
   - **Username** → `UPSTASH_KAFKA_USERNAME`
   - **Password** → `UPSTASH_KAFKA_PASSWORD`

### 2. Bonsai Elasticsearch (free tier)
1. Go to [bonsai.io](https://bonsai.io) → sign up → create a free "Sandbox" cluster.
2. From the cluster dashboard, copy the **Cluster URL** (it already includes
   credentials, looks like `https://user:pass@yourcluster.bonsaisearch.net:443`)
   → this is your `BONSAI_URL`.

### 3. Grafana Cloud (free tier)
1. Go to [grafana.com](https://grafana.com) → sign up for the free tier.
2. In your Grafana Cloud stack → **Connections** → **Add new connection** →
   search "Elasticsearch".
3. Paste your Bonsai URL as the data source URL, and the same
   username/password Bonsai gave you, in the connection's Auth section.
4. Set index name to `logs` (or `metric_windows`/`alerts` for other panels —
   you'll likely want 3 separate Elasticsearch data source connections, one
   per index, since Grafana's ES data source is tied to one index pattern).
5. Click **Save & Test** — it should confirm the connection works.

---

## Part 2 — Test everything locally first (before deploying)

Don't deploy blind — confirm the whole chain works on your machine using the
real Upstash/Bonsai credentials, before pushing to Render.

```bash
cd demo-app
pip install -r requirements.txt

export UPSTASH_KAFKA_BOOTSTRAP_SERVER="your-endpoint:9092"
export UPSTASH_KAFKA_USERNAME="your-username"
export UPSTASH_KAFKA_PASSWORD="your-password"
export APP_LOG_PATH=/tmp/app.log

python3 app.py
```

In another terminal:
```bash
cd pipeline
pip install -r requirements.txt

export UPSTASH_KAFKA_BOOTSTRAP_SERVER="your-endpoint:9092"
export UPSTASH_KAFKA_USERNAME="your-username"
export UPSTASH_KAFKA_PASSWORD="your-password"
export BONSAI_URL="https://user:pass@yourcluster.bonsaisearch.net:443"

python3 worker.py
```

In a third terminal, generate real load:
```bash
cd pipeline
pip install -r requirements-dev.txt
locust -f locustfile.py --host http://localhost:5000
```
Open `http://localhost:8089`, set number of users (try 20) and spawn rate
(try 5), click **Start swarming**.

Check it's working:
```bash
curl http://localhost:5001/health
curl http://localhost:5001/alerts
```

Give it 5–10 minutes of sustained load so the anomaly detector has enough
history to establish a baseline, then check `/alerts` again — with enough
load, the demo app's genuine flaky `/checkout` endpoint (5% real timeout
rate, 4% real bad-gateway rate) should eventually trip a real alert.

---

## Part 3 — Deploy to Render

### Deploy the demo app
1. Push this project to a GitHub repo.
2. In Render dashboard → **New** → **Web Service** → connect your repo.
3. Root directory: `demo-app`
4. Environment: Docker (Render will use the Dockerfile automatically).
5. Add environment variables: `UPSTASH_KAFKA_BOOTSTRAP_SERVER`,
   `UPSTASH_KAFKA_USERNAME`, `UPSTASH_KAFKA_PASSWORD`.
6. Instance type: Free.
7. Deploy. Note the public URL Render gives you (e.g.
   `https://your-app.onrender.com`).

### Deploy the pipeline worker
1. In Render → **New** → **Web Service** → same repo.
2. Root directory: `pipeline`
3. Environment: Docker.
4. Add environment variables: `UPSTASH_KAFKA_BOOTSTRAP_SERVER`,
   `UPSTASH_KAFKA_USERNAME`, `UPSTASH_KAFKA_PASSWORD`, `BONSAI_URL`.
5. Instance type: Free.
6. Deploy.

### Keep both services awake
Render's free web services spin down after 15 minutes of no traffic. Use
[UptimeRobot](https://uptimerobot.com) (also free) to ping both services'
`/health` endpoints every 5 minutes, so they stay warm — this is also a
completely normal, real pattern for free-tier deployments.

---

## Part 4 — Generate real demo traffic against your deployed app

```bash
cd pipeline
locust -f locustfile.py --host https://your-app.onrender.com
```
Open `http://localhost:8089`, start swarming. This hits your **live,
deployed** app with real traffic — perfect for a demo video or live
interview walkthrough.

---

## Part 5 — Build your Grafana dashboard

Once data is flowing, in Grafana Cloud:
1. **New Dashboard** → **Add panel**.
2. Panel 1 — "Error rate by service": data source = your `metric_windows`
   Elasticsearch connection, metric = average of `error_rate`, group by
   `service.keyword`, over time.
3. Panel 2 — "Latency by service": same index, metric = average of
   `avg_latency_ms`, group by `service.keyword`.
4. Panel 3 — "Active alerts": data source = your `alerts` connection, table
   visualization, filter `resolved: false`.
5. Save the dashboard, name it something like "Incident Monitor".

---

## Honest limitations (know these before an interview asks)

- Free-tier Kafka/Elasticsearch have message/storage caps — fine for a demo,
  would need paid tiers for real production volume.
- Render free services spin down without traffic; the UptimeRobot workaround
  is a real but imperfect fix — a paid Render plan removes this entirely.
- The anomaly detector is a single-metric rolling z-score — good baseline,
  explainable, but doesn't catch correlated multi-metric anomalies the way
  Isolation Forest or an autoencoder would.
- Running consumer + detector as threads in one process (instead of separate
  workers) means if one thread crashes badly enough to kill the process, both
  go down together — acceptable for a demo, not how you'd design it with a
  real ops budget.

Being able to say these limitations out loud, unprompted, is itself a strong
signal in an interview — it shows you understand trade-offs, not just that
you can make something work.
