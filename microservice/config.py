from prometheus_client import Counter, Gauge
import logging

CONSUL = "http://consul:8500"
LEADER_KEY = "microservice/leader"
SESSION_TTL = "30s"  # session Time-To-Live
RENEW_INTERVAL = 10  # seconds between renews

BASE = RENEW_INTERVAL  # 10
JITTER_FACTOR = 0.1  # 10%

SAVE_COUNTER = Counter("save_events_total", "Total number of SAVE events")
UPDATE_COUNTER = Counter("update_events_total", "Total number of UPDATE events")
ERROR_COUNTER = Counter("error_events_total", "Total number of ERROR events")

SAVE_GAUGE = Gauge("save_events_clickhouse", "Count of SAVE events in ClickHouse")
UPDATE_GAUGE = Gauge("update_events_clickhouse", "Count of UPDATE events in ClickHouse")
ERROR_GAUGE = Gauge("error_events_clickhouse", "Count of ERROR events in ClickHouse")
TOTAL_EVENTS_GAUGE = Gauge(
    "total_events_clickhouse", "Total count of events in ClickHouse"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
