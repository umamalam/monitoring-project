"""
locustfile.py
Real load-testing tool (industry standard: same tool companies use to load
test before launches). Simulates real users browsing, logging in, searching,
and checking out -- with realistic weighting (most users browse/search,
fewer actually check out, exactly like real traffic).

Checkout is weighted higher than a typical funnel would suggest on its own
-- deliberately, because /checkout is the only endpoint that can leak a
connection (see app.py's acquire_connection(leak_prone=True)). A realistic
1-in-10 checkout rate would take a very long sustained run before the pool
visibly trends anywhere; this weighting gets a demo to a genuine, visible
"connections climbing toward the limit" trend inside a reasonable test
window, while still leaving checkout as a minority of total traffic.

Run with:  locust -f locustfile.py --host http://localhost:5000
Then open http://localhost:8089 to control the load test from a web UI
(set number of users + spawn rate, start/stop live). For the connection-
leak / prediction demo specifically: run with at least 30-40 users for
20-30 minutes so the pipeline's 1-minute windows build up enough history
for forecaster.py to fit a trend (it needs MIN_POINTS=5 windows minimum,
HOLT_MIN_POINTS=20 windows to use the better Holt model) and for the
predictor to have something worth forecasting.
"""

import random
from locust import HttpUser, task, between


class RealUser(HttpUser):
    wait_time = between(0.5, 2.5)  # real users pause between actions

    @task(10)
    def browse_products(self):
        self.client.get("/products")

    @task(6)
    def search(self):
        query = random.choice(["shoes", "laptop", "phone", "a", "headphones", ""])
        self.client.get(f"/search?q={query}")

    @task(3)
    def login(self):
        self.client.post("/login", json={"username": f"user{random.randint(1, 10000)}"})

    @task(4)
    def checkout(self):
        # Weighted up from a "realistic" 1 to 4 -- see module docstring for
        # why. Still a minority of total traffic, just enough of it to make
        # the connection leak visible within a normal demo/test window.
        self.client.post("/checkout")

    @task(4)
    def add_product(self):
        # simulates admin/seller activity, also what makes /products genuinely
        # get slower over time (real unindexed-scan bug in the demo app)
        self.client.post("/products")
