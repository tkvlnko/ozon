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

            # 1) Unique sellers
            unique_sellers = client.execute(
                """
                SELECT uniq(company_id)
                FROM company_statistic_daily_all
                WHERE fromUnixTimestamp64Nano(ts) >= toStartOfInterval(now(), INTERVAL 1 day)
                """
            )[0][0]
            GAUGES["unique_sellers"].set(unique_sellers)

            # 2) Avg, Stddev и Upper Bound for attempt times
            (
                avg_,
                stddev_,
            ) = client.execute(
                """
                SELECT
                    avg(attempt),
                    stddevPop(attempt)
                FROM attempt_create_time_all
                """
            )[0]
            GAUGES["avg_attempt"].set(avg_)
            GAUGES["stddev_attempt"].set(stddev_)
            GAUGES["upper_bound_attempt"].set(avg_ + 3 * stddev_)

            # 3) Считаем failure_rate за последнюю минуту
            failure_rate = client.execute(
                    """
                    SELECT
                    sum(if(is_created, 0, 1)) * 100.0 / count()
                    FROM attempt_create_time_all
                    WHERE date_create >= now() - INTERVAL 1 MINUTE
                        """
                    )[0][0]
            GAUGES["failure_rate"].set(failure_rate)

            rows = client.execute("""
                SELECT
                if(country != 'RU', 'Глобал', 'РФ') AS segment,
                uniq(item_id) AS cnt
                FROM attempt_create_time_all
                WHERE is_created
                AND date_create >= now() - INTERVAL 1 MINUTE
                GROUP BY segment
            """)

            # Сброс прошлых значений, чтобы не держать старые лэйблы
            GAUGES["created_items"].clear()

            # Устанавливаем текущее значение по лэйблам
            for segment, cnt in rows:
                GAUGES["created_items"].labels(segment=segment).set(cnt)

        except Exception as e:
            logger.exception("❌ Error computing metrics")

        stop_metrics.wait(10)
