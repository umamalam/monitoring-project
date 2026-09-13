"""
locustfile.py
Real load-testing tool (industry standard: same tool companies use to load
test before launches). Simulates real users browsing, logging in, searching,
and checking out -- with realistic weighting (most users browse/search,
fewer actually check out, exactly like real traffic).

Run with:  locust -f locustfile.py --host http://localhost:5000
Then open http://localhost:8089 to control the load test from a web UI
(set number of users + spawn rate, start/stop live).
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

    @task(1)
    def checkout(self):
        # fewer users actually complete checkout, like real e-commerce funnels
        self.client.post("/checkout")

    @task(4)
    def add_product(self):
        # simulates admin/seller activity, also what makes /products genuinely
        # get slower over time (real unindexed-scan bug in the demo app)
        self.client.post("/products")
