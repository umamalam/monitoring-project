"""
console_consumer.py
Reads real-time logs from the app-logs Kafka topic (Redpanda) and prints a
rolling per-endpoint view: error rate % and average latency, over the last
60 seconds.
"""

import json
import os
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer

TOPIC = os.environ.get("KAFKA_TOPIC", "app-logs")
WINDOW_SECONDS = 60
PRINT_INTERVAL = 5


def get_consumer():
    return Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": os.environ["KAFKA_USERNAME"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "group.id": "console-consumer",
        "auto.offset.reset": "latest",
    })


def print_stats(events):
    now = time.time()
    while events and now - events[0][0] > WINDOW_SECONDS:
        events.popleft()

    per_endpoint = defaultdict(lambda: {"count": 0, "errors": 0, "latencies": []})
    for _ts, event in events:
        ep = event.get("endpoint", "unknown")
        per_endpoint[ep]["count"] += 1
        if event.get("level") == "ERROR":
            per_endpoint[ep]["errors"] += 1
        if "latency_ms" in event:
            per_endpoint[ep]["latencies"].append(event["latency_ms"])

    os.system("clear")
    print(f"=== Live stats (last {WINDOW_SECONDS}s) ===  {len(events)} events buffered\n")
    print(f"{'Endpoint':15s} {'Requests':>10s} {'Error %':>10s} {'Avg Latency':>14s}")
    print("-" * 55)
    for ep, stats in sorted(per_endpoint.items()):
        count = stats["count"]
        error_pct = (stats["errors"] / count * 100) if count else 0
        avg_latency = sum(stats["latencies"]) / len(stats["latencies"]) if stats["latencies"] else 0
        flag = "  <-- HIGH ERROR RATE" if error_pct > 20 else ""
        print(f"{ep:15s} {count:>10d} {error_pct:>9.1f}% {avg_latency:>12.1f}ms{flag}")
    print("\n(Ctrl+C to stop)")


def main():
    consumer = get_consumer()
    consumer.subscribe([TOPIC])
    print(f"Connected. Subscribed to '{TOPIC}'. Waiting for messages...")

    events = deque()
    last_print = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    events.append((time.time(), event))
                except Exception as e:
                    print(f"WARNING: could not parse message: {e}")
            elif msg is not None and msg.error():
                print(f"Kafka error: {msg.error()}")

            if time.time() - last_print >= PRINT_INTERVAL:
                print_stats(events)
                last_print = time.time()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
