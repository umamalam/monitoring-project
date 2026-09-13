"""
load_generator.py
Continuously sends realistic mixed traffic to the demo-app (app.py) so that
app-logs topic in Redpanda gets a meaningful volume of data to work with,
instead of just 3-4 one-off curl requests.

Usage:
    python3 load_generator.py
    python3 load_generator.py --requests 500 --delay 0.2
    python3 load_generator.py --duration 120   # run for 2 minutes instead

Requires: requests  (pip install requests)
"""

import argparse
import random
import string
import time

import requests

BASE_URL = "http://localhost:5000"

# Weighted mix of endpoints -- mimics real traffic patterns:
# most traffic browses products/search, fewer people actually login/checkout.
ENDPOINT_WEIGHTS = {
    "products_get": 40,
    "search": 25,
    "login": 15,
    "checkout": 12,
    "products_post": 5,
    "health": 3,
}


def random_username():
    return "user_" + "".join(random.choices(string.ascii_lowercase, k=6))


def random_query():
    words = ["shoes", "laptop", "phone", "t-shirt", "bag", "watch", "a", ""]
    return random.choice(words)


def hit_endpoint(name: str):
    try:
        if name == "products_get":
            r = requests.get(f"{BASE_URL}/products", timeout=5)
        elif name == "products_post":
            r = requests.post(f"{BASE_URL}/products", timeout=5)
        elif name == "login":
            r = requests.post(
                f"{BASE_URL}/login",
                json={"username": random_username()},
                timeout=5,
            )
        elif name == "checkout":
            r = requests.post(f"{BASE_URL}/checkout", timeout=5)
        elif name == "search":
            r = requests.get(f"{BASE_URL}/search", params={"q": random_query()}, timeout=5)
        elif name == "health":
            r = requests.get(f"{BASE_URL}/health", timeout=5)
        else:
            return None
        return r.status_code
    except requests.exceptions.RequestException as e:
        return f"ERR({e.__class__.__name__})"


def weighted_choice(weights: dict):
    names = list(weights.keys())
    wts = list(weights.values())
    return random.choices(names, weights=wts, k=1)[0]


def main():
    parser = argparse.ArgumentParser(description="Generate load against the demo app")
    parser.add_argument("--requests", type=int, default=200, help="Total number of requests to send")
    parser.add_argument("--duration", type=int, default=None, help="Run for N seconds instead of a fixed count")
    parser.add_argument("--delay", type=float, default=0.15, help="Base delay between requests (seconds)")
    parser.add_argument("--jitter", type=float, default=0.15, help="Random extra delay added to base delay")
    args = parser.parse_args()

    print(f"Target: {BASE_URL}")
    print("Starting load generation... (Ctrl+C to stop)\n")

    sent = 0
    start_time = time.time()
    counts = {}

    try:
        while True:
            if args.duration is not None:
                if time.time() - start_time >= args.duration:
                    break
            elif sent >= args.requests:
                break

            endpoint = weighted_choice(ENDPOINT_WEIGHTS)
            status = hit_endpoint(endpoint)
            counts[endpoint] = counts.get(endpoint, 0) + 1
            sent += 1

            print(f"[{sent}] {endpoint:15s} -> {status}")

            time.sleep(args.delay + random.uniform(0, args.jitter))

    except KeyboardInterrupt:
        print("\nStopped by user.")

    elapsed = time.time() - start_time
    print("\n--- Summary ---")
    print(f"Total requests sent : {sent}")
    print(f"Elapsed time        : {elapsed:.1f}s")
    for name, c in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {name:15s}: {c}")


if __name__ == "__main__":
    main()
