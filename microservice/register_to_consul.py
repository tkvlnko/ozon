import socket
import requests


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
    requests.put("http://consul:8500/v1/agent/service/register", json=svc)
    print(f"Registered {svc['ID']} to Consul", flush=True)
