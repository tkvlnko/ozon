import socket
import requests
import time
import random
import json
import os
import threading
from datetime import datetime
from clickhouse_driver import Client
from config import logger, GAUGES, SESSION_TTL, CONSUL, LEADER_KEY, BASE, JITTER_FACTOR
from connect import get_client

stop_metrics = threading.Event()
metrics_thread = None


# -----------------------------
# Leader Election Helpers
# -----------------------------
def create_session():
    payload = {
        "Name": f"leader-election-{socket.gethostname()}",
        "TTL": SESSION_TTL,
        "Behavior": "delete",
    }
    try:
        res = requests.put(f"{CONSUL}/v1/session/create", json=payload)
        res.raise_for_status()
        session_id = res.json()["ID"]
        logger.info(f"🟢 Created session {session_id}")
        return session_id
    except Exception as e:
        logger.exception("❌ Failed to create session")
        raise


def acquire_lock(session_id):
    try:
        params = {"acquire": session_id}
        res = requests.put(
            f"{CONSUL}/v1/kv/{LEADER_KEY}", params=params, data=socket.gethostname()
        )
        res.raise_for_status()
        got = res.json()
        logger.debug(
            f"🔐 Lock {'acquired' if got else 'not acquired'} for session {session_id}"
        )
        return got
    except Exception as e:
        logger.exception("❌ Failed to acquire lock")
        raise


def renew_session(session_id):
    try:
        res = requests.put(f"{CONSUL}/v1/session/renew/{session_id}")
        success = res.status_code == 200
        logger.debug(
            f"🔄 Renew session {session_id} {'succeeded' if success else 'failed'}"
        )
        return success
    except Exception as e:
        logger.exception("❌ Error renewing session")
        return False


def on_become_leader():
    global metrics_thread, stop_metrics
    if metrics_thread and metrics_thread.is_alive():
        return
    stop_metrics.clear()
    metrics_thread = threading.Thread(target=compute_metrics, daemon=True)
    metrics_thread.start()
    logger.info(
        f"👑 {socket.gethostname()} → became LEADER and started compute_metrics"
    )


def on_lose_leadership():
    stop_metrics.set()
    logger.warning(
        f"👋 {socket.gethostname()} → lost leadership and stopped compute_metrics"
    )


def leader_election_loop():
    session_id = create_session()
    is_leader = False
    while True:
        try:
            got = acquire_lock(session_id)
            if got and not is_leader:
                is_leader = True
                on_become_leader()
            elif not got and is_leader:
                is_leader = False
                on_lose_leadership()

            if not renew_session(session_id):
                logger.warning(f"⚠️ Session expired: {session_id}, creating new one")
                session_id = create_session()
                if is_leader:
                    is_leader = False
                    on_lose_leadership()

        except Exception as e:
            logger.exception("⚠️ Leader election loop error")

        delta = BASE * JITTER_FACTOR
        sleep_time = BASE + random.uniform(-delta, delta)
        time.sleep(sleep_time)


def compute_metrics():
    try:
        client = get_client()
    except Exception as e:
        logger.exception("❌ Failed to connect to ClickHouse in compute_metrics")
        return

    while not stop_metrics.is_set():
        try:

            unique_sellers = client.execute(
                """select
                    uniq(company_id)
                        from company_statistic_daily_all
                where
                fromUnixTimestamp64Nano(ts) >= toStartOfInterval(now(), interval 1 day)"""
            )[0][0]
            GAUGES["unique_sellers"].set(unique_sellers)

            first_time_sellers = client.execute(
                "SELECT uniqIf(company_id, frst = al) "
                "FROM ("
                "SELECT "
                "fromUnixTimestamp64Nano(ts) AS newTS, "
                "company_id, "
                "sum(c1) AS frst, "
                "sum(arraySum(mapValues(c2))) AS al "
                "FROM ("
                "SELECT "
                "fromUnixTimestamp64Nano(ts) AS newTS, "
                "company_id, "
                "uniqMerge(first_time) AS c1, "
                "mapFilter((k, v) -> (k NOT LIKE '%update/%'), uniqMapMerge(origins)) AS c2 "
                "FROM company_statistic_monthly "
                "WHERE $__dateFilter(newTS) "
                "GROUP BY newTS, company_id"
                ") t0 "
                "GROUP BY newTS, company_id"
                ") t "
                "GROUP BY newTS"
            )[0][0]
            GAUGES["first_time_sellers"].set(first_time_sellers)

            repeat_sellers = client.execute(
                "SELECT uniqIf(company_id, frst = 0) "
                "FROM ("
                "SELECT "
                "fromUnixTimestamp64Nano(ts) AS newTS, "
                "company_id, "
                "sum(c1) AS frst "
                "FROM ("
                "SELECT "
                "fromUnixTimestamp64Nano(ts) AS newTS, "
                "company_id, "
                "uniqMerge(first_time) AS c1 "
                "FROM company_statistic_monthly "
                "WHERE $__dateFilter(newTS) "
                "GROUP BY newTS, company_id"
                ") t0 "
                "GROUP BY newTS, company_id"
                ") t"
            )[0][0]
            GAUGES["repeat_sellers"].set(repeat_sellers)

            avg_full_time = client.execute(
                "SELECT avg(full_time) "
                "FROM attempt_create_time "
                "WHERE $__timeFilter(date_create)"
            )[0][0]
            GAUGES["avg_full_time"].set(avg_full_time)

            stddev_full_time = client.execute(
                "SELECT stddevPop(full_time) "
                "FROM attempt_create_time "
                "WHERE $__timeFilter(date_create)"
            )[0][0]
            GAUGES["stddev_full_time"].set(stddev_full_time)

            upper_bound_full_time = client.execute(
                "SELECT avg(full_time) + 3 * stddevPop(full_time) "
                "FROM attempt_create_time "
                "WHERE $__timeFilter(date_create)"
            )[0][0]
            GAUGES["upper_bound_full_time"].set(upper_bound_full_time)

            items_created = client.execute(
                "SELECT uniq(item_id) " "FROM attempt_create_time " "WHERE is_created"
            )[0][0]
            GAUGES["items_created"].set(items_created)

            total_attempts = client.execute(
                "SELECT sum(originRow.2) "
                "FROM ("
                "SELECT company_id, arrayJoin(uniqMapMerge(tryItemOrigins)) AS originRow "
                "FROM item_change_log_stat "
                "WHERE fromUnixTimestamp64Nano(ts) >= toStartOfInterval(now(), INTERVAL 1 DAY) "
                "GROUP BY company_id"
                ") t"
            )[0][0]
            GAUGES["total_attempts"].set(total_attempts)

            p90_load_time = client.execute(
                "WITH quantileTiming(0.90)(cnt) AS avg_load_time "
                "SELECT "
                "toStartOfInterval(hour, INTERVAL 30 MINUTE) AS at, "
                "(avg_load_time / lagInFrame(avg_load_time) OVER (ORDER BY at) - 1) * -100 AS pct_change "
                "FROM ("
                "SELECT toStartOfMinute(date_create) AS hour, uniq(item_id) AS cnt "
                "FROM attempt_create_time "
                "WHERE $__timeFilter(date_create) AND is_created "
                "GROUP BY hour ORDER BY hour"
                ") t0 "
                "GROUP BY at ORDER BY at"
            )[0][1]
            GAUGES["p90_load_time"].set(p90_load_time)

            top_attempts_per_seller = client.execute(
                "SELECT toDate(newTS) AS day, company_id, attempts, originAttempts "
                "FROM ("
                "SELECT "
                "toStartOfInterval(fromUnixTimestamp64Nano(ts), INTERVAL 1 DAY) AS newTS, "
                "company_id, "
                "arraySum(mapValues(uniqMapMerge(origins))) AS attempts, "
                "mapSort((k,v) -> -v, uniqMapMerge(origins)) AS originAttempts, "
                "row_number() OVER (PARTITION BY newTS ORDER BY attempts DESC) AS rankAttempts "
                "FROM item_change_log_stat "
                "GROUP BY newTS, company_id"
                ") t "
                "WHERE $__dateFilter(t.newTS) AND rankAttempts <= 10 "
                "ORDER BY t.newTS DESC, rankAttempts "
                "LIMIT 100"
            )
            GAUGES["top_attempts_per_seller"].set(top_attempts_per_seller)

        except Exception as e:
            logger.exception("❌ Error computing metrics")

        stop_metrics.wait(10)


# def compute_metrics_old():
#     # client = Client(host="clickhouse", port=9000, user="default", password="default")
#     host = os.getenv("CLICKHOUSE_HOST", "clickhouse-1")
#     port = int(os.getenv("CLICKHOUSE_PORT", 9000))
#     client = Client(
#         host=host, port=port, user="default", password="default", database="item_upload"
#     )
#     while not stop_metrics.is_set():
#         # try:
#         #     save_count = client.execute(
#         #         "SELECT count() FROM product_load_events WHERE event='SAVE'"
#         #     )[0][0]
#         #     update_count = client.execute(
#         #         "SELECT count() FROM product_load_events WHERE event='UPDATE'"
#         #     )[0][0]
#         #     error_count = client.execute(
#         #         "SELECT count() FROM product_load_events WHERE event='ERROR'"
#         #     )[0][0]
#         #     total_count = client.execute("SELECT count() FROM product_load_events")[0][
#         #         0
#         #     ]
#         #     SAVE_GAUGE.set(save_count)
#         #     UPDATE_GAUGE.set(update_count)
#         #     ERROR_GAUGE.set(error_count)
#         #     TOTAL_EVENTS_GAUGE.set(total_count)
#         # except Exception as e:
#         #     print("Error computing metrics:", e, flush=True)
#         # stop_metrics.wait(10)
#         pass
