"""
Это микро сервис

в начале стоит time sleep,  чтобы при запуске контейнеров с нуля кликхаус успел включиться

важно что код сам автоматически регистрирует все инстансы в консуле
"""

import socket
import time
import random
import json
import threading
from datetime import datetime
import time
from prometheus_client import start_http_server, Counter, Gauge
from clickhouse_driver import Client
import requests


print("Time sleep begun", flush=True)
time.sleep(20)
print("process begun")


# -----------------------------
# Prometheus Metrics Definitions
# -----------------------------
SAVE_COUNTER = Counter("save_events_total", "Total number of SAVE events")
UPDATE_COUNTER = Counter("update_events_total", "Total number of UPDATE events")
ERROR_COUNTER = Counter("error_events_total", "Total number of ERROR events")

# Gauges, которые обновляем, опрашивая ClickHouse
SAVE_GAUGE = Gauge("save_events_clickhouse", "Count of SAVE events in ClickHouse")
UPDATE_GAUGE = Gauge("update_events_clickhouse", "Count of UPDATE events in ClickHouse")
ERROR_GAUGE = Gauge("error_events_clickhouse", "Count of ERROR events in ClickHouse")
TOTAL_EVENTS_GAUGE = Gauge(
    "total_events_clickhouse", "Total count of events in ClickHouse"
)

log_counter = 0


def simulate_event():
    event_types = ["SAVE", "UPDATE", "ERROR"]
    event_type = random.choices(event_types, weights=[0.5, 0.4, 0.1])[0]

    now = datetime.now()

    seller_id = random.randint(1000, 9999)
    production_countries = ["CN", "US", "RU", "DE"]
    production_country = random.choice(production_countries)

    upload_method = "mass-create/json"
    upload_file_hash = hex(random.getrandbits(128))[2:]
    import_batch_id = random.randint(10000, 99999)

    # Симуляция загрузки медиафайлов (от 0 до 5 файлов)
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
        "ts": int(now.timestamp() * 1e9),  # наносекундная метка
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
    return event


def process_event(event):
    # Инкрементим нужный Prometheus Counter в зависимости от типа события.
    global log_counter
    event_type = event.get("event")
    if event_type == "SAVE":
        SAVE_COUNTER.inc()
    elif event_type == "UPDATE":
        UPDATE_COUNTER.inc()
    elif event_type == "ERROR":
        ERROR_COUNTER.inc()

    # log_counter += 1
    # if log_counter % 5 == 0:
    #     print("Processed Event:\n", json.dumps(event, indent=2))
    #     log_counter = 0


def compute_metrics():
    """
    Отдельный поток, который каждые 10 секунд опрашивает ClickHouse
    и выставляет значения в Gauge-метрики на основе данных из БД.
    """
    client = Client(host="clickhouse", port=9000, user="default", password="default")

    while True:
        try:
            save_count = client.execute(
                "SELECT count() FROM product_load_events WHERE event='SAVE'"
            )[0][0]
            update_count = client.execute(
                "SELECT count() FROM product_load_events WHERE event='UPDATE'"
            )[0][0]
            error_count = client.execute(
                "SELECT count() FROM product_load_events WHERE event='ERROR'"
            )[0][0]
            total_count = client.execute("SELECT count() FROM product_load_events")[0][
                0
            ]

            SAVE_GAUGE.set(save_count)
            UPDATE_GAUGE.set(update_count)
            ERROR_GAUGE.set(error_count)
            TOTAL_EVENTS_GAUGE.set(total_count)

            time.sleep(10)
        except Exception as e:
            print("Error computing metrics from ClickHouse:", e)
            time.sleep(10)


def register_to_consul():
    instance_id = socket.gethostname()  # обычно имя контейнера уникально
    svc = {
        "ID": f"microservice-{instance_id}",  # ← уникальный ID
        "Name": "microservice",  # ← одно имя, по которому группируем
        "Address": instance_id,  # или host.docker.internal внутри Docker
        "Port": 84,
        "Tags": ["metrics"],
        "Check": {"HTTP": f"http://{instance_id}:84/metrics", "Interval": "10s"},
    }
    requests.put("http://consul:8500/v1/agent/service/register", json=svc)
    print(f"Registered {svc['ID']} to Consul", flush=True)


def main():
    start_http_server(84)
    print("Prometheus metrics server started on port 8000")

    client = Client(host="clickhouse", port=9000, user="default", password="default")

    # Создаём таблицу, если её ещё нет
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS product_load_events (
            ts DateTime64(9),
            item_id UInt64,
            event String,
            state String,
            attempt_id String,
            data String
        ) ENGINE = MergeTree()
        ORDER BY ts
    """
    )

    # Запускаем поток, который будет опрашивать ClickHouse и обновлять Gauges
    metrics_thread = threading.Thread(target=compute_metrics, daemon=True)
    metrics_thread.start()

    # Основной цикл: генерируем и обрабатываем событие, записываем в ClickHouse
    while True:
        event = simulate_event()
        process_event(event)

        # Преобразуем наносекундную метку во внутренний формат Python
        event_dt = datetime.fromtimestamp(event["ts"] / 1e9).astimezone()

        # Записываем в ClickHouse
        client.execute(
            "INSERT INTO product_load_events (ts, item_id, event, state, attempt_id, data) VALUES",
            [
                (
                    event_dt,
                    event["item_id"],
                    event["event"],
                    event["state"],
                    event["attempt_id"],
                    event["data"],
                )
            ],
        )

        # Небольшая задержка, чтобы не забивать базу слишком часто
        time.sleep(0.1)


if __name__ == "__main__":
    register_to_consul()
    main()
    # register_to_consul()
