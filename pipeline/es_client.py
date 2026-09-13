"""
es_client.py
Shared OpenSearch connection helper, pointed at Bonsai (a free-tier
managed search host -- despite the name "Bonsai Elasticsearch", it actually
runs OpenSearch under the hood, so we use opensearch-py, not elasticsearch-py,
to avoid client/server compatibility issues).
"""

import os
from urllib.parse import urlparse

from opensearchpy import OpenSearch

BONSAI_URL = os.environ.get("BONSAI_URL", "http://localhost:9200")


def get_es_client():
    parsed = urlparse(BONSAI_URL)
    return OpenSearch(
        hosts=[{"host": parsed.hostname, "port": parsed.port or 443}],
        http_auth=(parsed.username, parsed.password),
        use_ssl=parsed.scheme == "https",
        verify_certs=True,
    )


def ensure_indices(es):
    indices = {
        "logs": {
            "mappings": {"properties": {
                "timestamp": {"type": "date"},
                "service": {"type": "keyword"},
                "level": {"type": "keyword"},
                "endpoint": {"type": "keyword"},
                "status_code": {"type": "integer"},
                "latency_ms": {"type": "float"},
                "message": {"type": "text"},
            }}
        },
        "metric_windows": {
            "mappings": {"properties": {
                "window_start": {"type": "date"},
                "service": {"type": "keyword"},
                "request_count": {"type": "integer"},
                "error_count": {"type": "integer"},
                "error_rate": {"type": "float"},
                "avg_latency_ms": {"type": "float"},
                "p95_latency_ms": {"type": "float"},
            }}
        },
        "alerts": {
            "mappings": {"properties": {
                "created_at": {"type": "date"},
                "service": {"type": "keyword"},
                "metric": {"type": "keyword"},
                "value": {"type": "float"},
                "baseline": {"type": "float"},
                "z_score": {"type": "float"},
                "severity": {"type": "keyword"},
                "resolved": {"type": "boolean"},
                "message": {"type": "text"},
            }}
        },
    }
    for name, mapping_body in indices.items():
        if not es.indices.exists(index=name):
            es.indices.create(index=name, body=mapping_body)
            print(f"[es_client] created index '{name}'")
