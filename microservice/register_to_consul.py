import socket
import requests
from config import logger


def register_to_consul():
    instance_id = socket.gethostname()
    svc = {
        "ID": f"microservice-{instance_id}",
        "Name": "microservice",
        "Address": instance_id,
        "Port": 84,
        "Tags": ["metrics"],
        "Check": {"HTTP": f"http://{instance_id}:84/metrics", "Interval": "10s"},
    }
    try:
        requests.put("http://consul:8500/v1/agent/service/register", json=svc)
        logger.info(f"✅ Registered {svc['ID']} to Consul")
    except Exception as e:
        logger.exception("❌ Failed to register service to Consul")
