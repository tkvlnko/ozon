import socket
import time
import random
import json
import threading
from datetime import datetime
from prometheus_client import start_http_server, Counter, Gauge
from clickhouse_driver import Client
import requests
from register_to_consul import register_to_consul
from config import SAVE_COUNTER, UPDATE_COUNTER, ERROR_COUNTER
from compute_metrics import leader_election_loop

# -----------------------------
# Leader Election Configuration
# -----------------------------
# CONSUL = "http://consul:8500"
# LEADER_KEY = "microservice/leader"
# SESSION_TTL = "30s"  # session Time-To-Live
# RENEW_INTERVAL = 10  # seconds between renews

# BASE = RENEW_INTERVAL  # 10
# JITTER_FACTOR = 0.1  # 10%

# -----------------------------
# Prometheus Metrics Definitions
# -----------------------------
# SAVE_COUNTER = Counter("save_events_total", "Total number of SAVE events")
# UPDATE_COUNTER = Counter("update_events_total", "Total number of UPDATE events")
# ERROR_COUNTER = Counter("error_events_total", "Total number of ERROR events")

# SAVE_GAUGE = Gauge("save_events_clickhouse", "Count of SAVE events in ClickHouse")
# UPDATE_GAUGE = Gauge("update_events_clickhouse", "Count of UPDATE events in ClickHouse")
# ERROR_GAUGE = Gauge("error_events_clickhouse", "Count of ERROR events in ClickHouse")
# TOTAL_EVENTS_GAUGE = Gauge(
#     "total_events_clickhouse", "Total count of events in ClickHouse"
# )

# globals for metrics thread control


# stop_metrics = threading.Event()
# metrics_thread = None



# -----------------------------
# Existing Logic
# -----------------------------

print("Time sleep begun", flush=True)
time.sleep(20)
print("process begun", flush=True)


# module-level var to remember when we last printed
_last_debug_time = 0.0
_debug_interval = 3.0  # seconds


# def simulate_event():
#     event_types = ["SAVE", "UPDATE", "ERROR"]
#     event_type = random.choices(event_types, weights=[0.5, 0.4, 0.1])[0]
#     now = datetime.now()
#     seller_id = random.randint(1000, 9999)
#     production_countries = ["CN", "US", "RU", "DE"]
#     production_country = random.choice(production_countries)
#     upload_method = "mass-create/json"
#     upload_file_hash = hex(random.getrandbits(128))[2:]
#     import_batch_id = random.randint(10000, 99999)
#     num_media_files = random.randint(0, 5)
#     has_media = random.choice([True, False])
#     media_files = (
#         [
#             f"https://cdn.example.com/{random.randint(100000, 999999)}.jpg"
#             for _ in range(num_media_files)
#         ]
#         if has_media
#         else None
#     )
#     return {
#         "item_id": random.randint(1000000000, 9999999999),
#         "ts": int(now.timestamp() * 1e9),
#         "event": event_type,
#         "state": (
#             "imported"
#             if event_type == "SAVE"
#             else ("updated" if event_type == "UPDATE" else "error")
#         ),
#         "attempt_id": f"{random.randint(1000, 9999)}|{random.randint(100000, 999999)}",
#         "data": json.dumps(
#             {
#                 "info": "Simulated event data",
#                 "seller_id": seller_id,
#                 "production_country": production_country,
#                 "upload_method": upload_method,
#                 "upload_file_hash": upload_file_hash,
#                 "import_batch_id": import_batch_id,
#                 "media_files": media_files,
#             }
#         ),
#     }



def simulate_event():
    global _last_debug_time

    event_types = ["SAVE", "UPDATE", "ERROR"]
    event_type = random.choices(event_types, weights=[0.5, 0.4, 0.1])[0]
    now = datetime.now()
    seller_id = random.randint(1000, 9999)
    production_countries = ["CN", "US", "RU", "DE"]
    production_country = random.choice(production_countries)
    upload_method = "mass-create/json"
    upload_file_hash = hex(random.getrandbits(128))[2:]
    import_batch_id = random.randint(10000, 99999)
    num_media_files = random.randint(0, 5)
    has_media = random.choice([True, False])
    media_files = (
        [
            f"https://cdn.example.com/{random.randint(100000, 999999)}.jpg"
            for _ in range(num_media_files)
        ]
        if has_media
        else None
    )

    event = {
        "item_id": random.randint(1000000000, 9999999999),
        "ts": int(now.timestamp() * 1e9),
        "event": event_type,
        "state": (
            "imported"
            if event_type == "SAVE"
            else ("updated" if event_type == "UPDATE" else "error")
        ),
        "attempt_id": f"{random.randint(1000, 9999)}|{random.randint(100000, 999999)}",
        "data": json.dumps(
            {
                "info": "Simulated event data",
                "seller_id": seller_id,
                "production_country": production_country,
                "upload_method": upload_method,
                "upload_file_hash": upload_file_hash,
                "import_batch_id": import_batch_id,
                "media_files": media_files,
            }
        ),
    }

    # throttle debug prints to once every _debug_interval seconds
    now_ts = time.time()
    if now_ts - _last_debug_time >= _debug_interval:
        print(
            f"[DEBUG {datetime.now().isoformat()}] "
            f"event={event_type}, seller_id={seller_id}, country={production_country}, "
            f"batch={import_batch_id}, media_files={len(media_files) if media_files else 0}"
        )
        _last_debug_time = now_ts

    return event



def process_event(event):
    t = event.get("event")
    if t == "SAVE":
        SAVE_COUNTER.inc()
    elif t == "UPDATE":
        UPDATE_COUNTER.inc()
    elif t == "ERROR":
        ERROR_COUNTER.inc()


# def compute_metrics():
#     client = Client(host="clickhouse", port=9000, user="default", password="default")
#     while not stop_metrics.is_set():
#         try:
#             save_count = client.execute(
#                 "SELECT count() FROM product_load_events WHERE event='SAVE'"
#             )[0][0]
#             update_count = client.execute(
#                 "SELECT count() FROM product_load_events WHERE event='UPDATE'"
#             )[0][0]
#             error_count = client.execute(
#                 "SELECT count() FROM product_load_events WHERE event='ERROR'"
#             )[0][0]
#             total_count = client.execute("SELECT count() FROM product_load_events")[0][
#                 0
#             ]
#             SAVE_GAUGE.set(save_count)
#             UPDATE_GAUGE.set(update_count)
#             ERROR_GAUGE.set(error_count)
#             TOTAL_EVENTS_GAUGE.set(total_count)
#         except Exception as e:
#             print("Error computing metrics:", e, flush=True)
#         stop_metrics.wait(10)


def main():
    start_http_server(84)
    print("Prometheus metrics server started on port 84", flush=True)
    client = Client(host="clickhouse", port=9000, user="default", password="default")
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS product_load_events (
            ts DateTime64(9),
            item_id UInt64,
            event String,
            state String,
            attempt_id String,
            data String
        ) ENGINE = MergeTree() ORDER BY ts
    """
    )
    while True:
        ev = simulate_event()
        process_event(ev)
        ev_dt = datetime.fromtimestamp(ev["ts"] / 1e9)
        client.execute(
            "INSERT INTO product_load_events (ts, item_id, event, state, attempt_id, data) VALUES",
            [
                (
                    ev_dt,
                    ev["item_id"],
                    ev["event"],
                    ev["state"],
                    ev["attempt_id"],
                    ev["data"],
                )
            ],
        )
        time.sleep(0.1)


if __name__ == "__main__":
    register_to_consul()
    # старт элекшн-лупа в фоне
    threading.Thread(target=leader_election_loop, daemon=True).start()
    main()
