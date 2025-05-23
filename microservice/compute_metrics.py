import socket
import requests
import time
import random
import json
import os
import threading
from datetime import datetime
from config import *
from clickhouse_driver import Client
from config import logger

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
    # host = os.getenv("CLICKHOUSE_HOST", "clickhouse-1")
    # port = int(os.getenv("CLICKHOUSE_PORT", 9000))
    # try:
    #     client = Client(
    #         host=host, port=port, user="default", password="default", database="item_upload"
    #     )
    #     logger.info(f"📊 Connected to ClickHouse at {host}:{port}")
    # except Exception as e:
    #     logger.exception("❌ Failed to connect to ClickHouse in compute_metrics")
    #     return

    while not stop_metrics.is_set():
        try:
            # Пример метрик (если раскомментировать)
            # save_count = client.execute("SELECT count() FROM product_load_events WHERE event='SAVE'")[0][0]
            # SAVE_GAUGE.set(save_count)
            pass
        except Exception as e:
            logger.exception("❌ Error computing metrics")

        stop_metrics.wait(10)


def compute_metrics_old():
    # client = Client(host="clickhouse", port=9000, user="default", password="default")
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse-1")
    port = int(os.getenv("CLICKHOUSE_PORT", 9000))
    client = Client(
        host=host, port=port, user="default", password="default", database="item_upload"
    )
    while not stop_metrics.is_set():
        # try:
        #     save_count = client.execute(
        #         "SELECT count() FROM product_load_events WHERE event='SAVE'"
        #     )[0][0]
        #     update_count = client.execute(
        #         "SELECT count() FROM product_load_events WHERE event='UPDATE'"
        #     )[0][0]
        #     error_count = client.execute(
        #         "SELECT count() FROM product_load_events WHERE event='ERROR'"
        #     )[0][0]
        #     total_count = client.execute("SELECT count() FROM product_load_events")[0][
        #         0
        #     ]
        #     SAVE_GAUGE.set(save_count)
        #     UPDATE_GAUGE.set(update_count)
        #     ERROR_GAUGE.set(error_count)
        #     TOTAL_EVENTS_GAUGE.set(total_count)
        # except Exception as e:
        #     print("Error computing metrics:", e, flush=True)
        # stop_metrics.wait(10)
        pass
