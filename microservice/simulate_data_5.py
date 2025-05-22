import time
import os
import random
import re
import json
import threading
from datetime import datetime
from prometheus_client import start_http_server
from clickhouse_driver import Client

from register_to_consul import register_to_consul
from config import SAVE_COUNTER, UPDATE_COUNTER, ERROR_COUNTER
from compute_metrics import leader_election_loop

_last_debug_time = 0.0
_debug_interval = 3.0 

ITEM_POOL = [random.randint(1_000_000_000, 9_999_999_999) for _ in range(10_000)]
item_attempts = {}
MAX_ACTIVE_ITEMS = 10_000


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

    # Время события
    now = datetime.now()

    ts_ns = int(now.timestamp() * 1e9)  # **signed** nanoseconds epoch
    item_attempts[item_id].append(ts_ns)

    # Event taxonomy & probabilities ===================================
    event_types = ["SAVE", "UPDATE", "ERROR"]
    weights = [0.5, 0.4, 0.1]
    event_type = random.choices(event_types, weights=weights)[0]

    # Randomised identifiers & metadata
    company_id = random.randint(1000, 9999)
    category_id = random.randint(1, 500)

    production_countries = ["CN", "US", "RU", "DE"]
    production_country = random.choice(production_countries)

    # Media files simulation
    num_media_files = random.randint(0, 5)
    has_media = random.choice([True, False])
    media_files = (
        [
            f"https://cdn.example.com/{random.randint(100000,999999)}.jpg"
            for _ in range(num_media_files)
        ]
        if has_media
        else []
    )

    # event_type и is_created
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

    # Attempt/request identifiers
    attempt_id = f"{random.randint(1000,9999)}|{random.randint(100000,999999)}"
    request_id = f"{random.randint(100000,999999):x}"
    origin = random.choice(["upload/ui", "upload/api", "update/manual"])
    origin_id = str(random.randint(1, 100))

    # Build media_map: key = "<ext>:<url>"
    media_map: dict[str, int] = {}

    url_by_media: dict[str, int] = {}
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
        print(
            f"[DEBUG {datetime.now().isoformat()}] "
            f"event={event_type}, company_id={company_id}, country={production_country}",
            flush=True,
        )

        _last_debug_time = now_ts

    if is_created:
        up_ts = ts_ns + random.randint(5_000_000_000, 25_000_000_000)
        del item_attempts[item_id]
    else:
        up_ts = 0

    return {
        "item_id": random.randint(1000000000, 9999999999),
        "ts_ns": ts_ns,  # **store signed nanoseconds**
        "event": event_type,
        # "state": state,
        # "previous_state": previous_state,
        # "state_updated_at": ts_ns,
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
    t = event.get("event")
    if t == "SAVE":
        SAVE_COUNTER.inc()
    elif t == "UPDATE":
        UPDATE_COUNTER.inc()
    elif t == "ERROR":
        ERROR_COUNTER.inc()


def rebuild_schema(client: Client) -> None:
    ddl_file = "./ddl.sql"
    text = open(ddl_file, "r", encoding="utf-8").read()
    stmts = [s.strip() for s in text.split(";") if s.strip()]
    for stmt in stmts:
        low = stmt.lower()

        print("🚀 Executing:", stmt[:100])
        try:
            # Удаляем таблицу ПЕРЕД созданием, если она реплицируемая
            if "replicated" in low and "create table" in low:
                m = re.match(
                    r"create table\s+(?:if not exists\s+)?([^\s(]+)",
                    stmt,
                    re.IGNORECASE,
                )
                if m:
                    tbl_ident = m.group(1)
                    if "." in tbl_ident:
                        db_part = tbl_ident.split(".", 1)[0]
                        db_name = db_part.strip('`"')
                        client.execute(
                            f"CREATE DATABASE IF NOT EXISTS {db_name} ON CLUSTER local_cluster"
                        )
                    # client.execute(
                    #     f"DROP TABLE IF EXISTS {tbl_ident} ON CLUSTER local_cluster"
                    # )

            client.execute(stmt)
        except Exception as e:
            msg = str(e)
            if "already exists" in msg or "Unknown table expression identifier" in msg:
                continue
            else:
                print(f"❌❌ Ошибка при выполнении:\n{stmt}\n→ {msg}")

        # for stmt in stmts:
        #     stmt = stmt.strip()
        #     low = stmt.lower()

        #     SKIP_MARKERS = (
        #         "with url_by_media as nested_map",   # daily_mv + status_daily_mv
        #     )

        #     for stmt in stmts:
        #         low = stmt.lower()
        #         if any(marker in low for marker in SKIP_MARKERS):
        #             continue            # не выполняем проблемную MV

        #         print("🚀 Executing:", stmt[:100])
        #         try:
        #             client.execute(stmt)
        #         except Exception as e:
        #             msg = str(e)
        #             msg = str(e)
        #             # Пропустить любые “already exists”
        #             if "already exists" in msg:
        #                 continue
        #             # Пропустить Unknown table expression для Distributed AS …
        #             if "Unknown table expression identifier" in msg:
        #                 continue
        #             else:
        #                 print(f"❌❌ Ошибка при выполнении:\n{stmt}\n→ {msg}")

        # Pass through any CREATE DATABASE / USE as-is
        if low.startswith("create database") or low.startswith("use "):
            client.execute(stmt)
            continue

        # Handle CREATE TABLE (with or without IF NOT EXISTS)
        if low.startswith("create table"):
            # Capture everything up to the first whitespace or "("
            # This includes `db`.`table`, "db"."table", or unquoted db.table
            m = re.match(
                r"create table\s+(?:if not exists\s+)?([^\s(]+)", stmt, re.IGNORECASE
            )
            if m:
                tbl_ident = m.group(1)  # e.g. `item_upload`.`company_statistic`
                # If it's qualified (db.table), ensure the DB exists
                if "." in tbl_ident:
                    db_part = tbl_ident.split(".", 1)[0]
                    db_name = db_part.strip('`"')
                #     client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                # client.execute(f"DROP TABLE IF EXISTS {tbl_ident}")
            client.execute(stmt)
            continue


def main() -> None:
    print("Time sleep begun", flush=True)
    time.sleep(25)
    print("process begun", flush=True)

    # Prometheus endpoint
    start_http_server(84)
    print("Prometheus metrics server started on port 84", flush=True)

    # Service registration & leader election
    register_to_consul()
    threading.Thread(target=leader_election_loop, daemon=True).start()

    # ClickHouse connection
    try:
        # clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse-1")
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", 9000))
        clickhouse_hosts = [
            "clickhouse-1",
            "clickhouse-2",
            "clickhouse-3",
            "clickhouse-4",
        ]
        ports = [9000, 9000, 9000, 9000]  

        client = Client(
            host="clickhouse-1",
            port=9000,
            alt_hosts="clickhouse-2:9000,clickhouse-3:9000,clickhouse-4:9000",
            round_robin=True,
            database="item_upload",
            user="default",
            password="default",
        )
        print(f"➡️ Writing to ClickHouse at {clickhouse_hosts}:{ports}")
        # client.execute("DROP DATABASE IF EXISTS item_upload SYNC")
        # client.execute("CREATE DATABASE item_upload ON CLUSTER local_cluster;")
        # client.execute("USE item_upload")

        # rebuild_schema(client)
    except Exception as e:
        print("❌ Trying to connect to click", repr(e))

    while True:
        ev = simulate_event()
        process_event(ev)

        ev_ts_ns = ev["ts_ns"]
        time.sleep(12)  #### НЕ МЕНЯТЬ ОСТАВИТЬ 10 сек


        try:
            client.execute(
                "INSERT INTO company_statistic_daily_buffer VALUES",
                [
                    (
                        ev_ts_ns,
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
            print("❌😭Problem with connection to clickhouse ", str(e)[:200])
            time.sleep(10)

        time.sleep(0.1)


if __name__ == "__main__":
    threading.Thread(target=leader_election_loop, daemon=True).start()
    main()
