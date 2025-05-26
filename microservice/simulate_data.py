import time
import os
import random
import re
import json
import threading
from datetime import datetime
from prometheus_client import start_http_server


from register_to_consul import register_to_consul
from config import logger
from compute_metrics import leader_election_loop
from connect import get_client

_last_debug_time = 0.0
_debug_interval = 3.0

ITEM_POOL = [random.randint(1_000_000_000, 9_999_999_999) for _ in range(1_000)]
item_attempts = {}
MAX_ACTIVE_ITEMS = 1_000


def simulate_event():
    global _last_debug_time

    if len(item_attempts) < MAX_ACTIVE_ITEMS or not item_attempts:
        item_id = random.choice(ITEM_POOL)
        if item_id not in item_attempts:
            item_attempts[item_id] = []
    else:
        try:
            item_id = random.choice(list(item_attempts.keys()))
        except IndexError:
            item_id = random.choice(ITEM_POOL)
            item_attempts[item_id] = []

    now = datetime.now()
    ts_ns = int(now.timestamp() * 1e9)
    item_attempts[item_id].append(ts_ns)

    event_types = ["SAVE", "UPDATE", "ERROR"]
    weights = [0.5, 0.4, 0.1]
    event_type = random.choices(event_types, weights=weights)[0]

    company_id = random.randint(1000, 9999)
    category_id = random.randint(1, 500)
    production_countries = ["CN", "US", "RU", "DE"]
    production_country = random.choice(production_countries)

    def generate_media_url() -> str:
        domain_type = random.choices(
            ["our", "external", "bad"],
            weights=[0.5, 0.4, 0.1],  # можно менять веса
            k=1
        )[0]

        file_id = random.randint(100000, 999999)

        if domain_type == "our":
            domain = random.choice(["ozone.ru", "ozonusercontent.com"])
        elif domain_type == "bad":
            domain = "bad_hostname"
        else:
            domain = random.choice(["imgur.com", "unsplash.com", "cdn.otherhost.net", "example.com"])

        return f"https://cdn.{domain}/{file_id}.jpg"

    # Генерация списка
    num_media_files = random.randint(0, 5)
    has_media = random.choice([True, False])

    media_files = [generate_media_url() for _ in range(num_media_files)] if has_media else []


    attempt_count = len(item_attempts[item_id])
    if attempt_count >= 3:
        is_created = random.random() < 0.8
    elif attempt_count == 2:
        is_created = random.random() < 0.4
    else:
        is_created = random.random() < 0.05

    event_type = (
        "SAVE"
        if is_created
        else random.choices(["UPDATE", "ERROR"], weights=[0.7, 0.3])[0]
    )

    attempt_id = f"{random.randint(1000,9999)}|{random.randint(100000,999999)}"
    request_id = f"{random.randint(100000,999999):x}"
    origin = random.choice(["upload/ui", "upload/api", "update/manual"])
    origin_id = str(random.randint(1, 100))

    media_map = {}
    url_by_media = {}
    for url in media_files:
        ext = url.rsplit(".", 1)[-1]
        key = f"{ext}:{url}"
        url_by_media[key] = url_by_media.get(key, 0) + 1

    data = json.dumps(
        {
            "info": "Simulated event data",
            "company_id": company_id,
            "production_country": production_country,
            "media_files": media_files,
        }
    )

    now_ts = time.time()
    if now_ts - _last_debug_time >= _debug_interval:
        logger.info(
            f"event={event_type}, company_id={company_id}, country={production_country}"
        )
        _last_debug_time = now_ts

    if is_created:
        up_ts = ts_ns + random.randint(5_000_000_000, 25_000_000_000)
        del item_attempts[item_id]
    else:
        up_ts = 0

    return {
        "item_id": item_id,
        "ts_ns": ts_ns,
        "event": event_type,
        "attempt_id": attempt_id,
        "request_id": request_id,
        "up_ts": up_ts,
        "origin": origin,
        "is_created": is_created,
        "origin_id": origin_id,
        "country": production_country,
        "data": data,
        "company_id": company_id,
        "category_id": category_id,
        "media": media_map,
        "url_by_media": url_by_media,
    }


def process_event(event: dict[str, str]) -> None:
    #     t = event.get("event")
    #     if t == "SAVE":
    #         SAVE_COUNTER.inc()
    #     elif t == "UPDATE":
    #         UPDATE_COUNTER.inc()
    #     elif t == "ERROR":
    #         ERROR_COUNTER.inc()
    pass


def main() -> None:
    logger.info("🕒 Time sleep begun")
    time.sleep(25)

    start_http_server(84)
    logger.info("📊 Prometheus metrics server started on port 84")

    register_to_consul()
    # threading.Thread(target=leader_election_loop, daemon=True).start()

    try:
        client = get_client()
    except Exception as e:
        logger.exception("❌ Trying to connect to ClickHouse")

    while True:
        ev = simulate_event()
        process_event(ev)
        try:
            client.execute(
                "INSERT INTO item_upload.company_statistic_daily_buffer VALUES",
                [
                    (
                        ev["ts_ns"],
                        ev["item_id"],
                        ev["company_id"],
                        ev["category_id"],
                        ev["origin"],
                        ev["media"],
                        ev["country"],
                        ev["url_by_media"],
                    )
                ],
            )
            client.execute(
                "INSERT INTO item_upload.company_statistic_status_daily_buffer VALUES",
                [
                    (
                        ev["ts_ns"],
                        ev["item_id"],
                        ev["event"] == "SAVE",
                        ev["up_ts"],
                    )
                ],
            )

        except Exception as e:
            logger.error(f"❌😭 Problem with connection to ClickHouse: {str(e)[:200]}")
            time.sleep(10)

        time.sleep(0.1)


if __name__ == "__main__":
    threading.Thread(target=leader_election_loop, daemon=True).start()
    main()
