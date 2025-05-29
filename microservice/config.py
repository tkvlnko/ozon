from prometheus_client import Counter, Gauge
import logging

CONSUL = "http://consul:8500"
LEADER_KEY = "microservice/leader"
SESSION_TTL = "30s"
COMPUTE_METRICS_INTERVAL = 10
METRICS_PORT = 84
POLL_INTERVAL = 60

BASE = COMPUTE_METRICS_INTERVAL  # 10
JITTER_FACTOR = 0.1  # 10%

CH_PASSWORD = "default"
CH_USER = "default"
CH_PORT = 9000


GAUGES = {
    "unique_sellers": Gauge(
        "company_statistic_daily_unique_sellers",
        "Unique count of company_id in company_statistic_daily_all over the last day",
    ),
    "avg_attempt": Gauge(
        "company_statistic_daily_avg_attempt_time",
        "Average attempt time in attempt_create_time_all over the last day",
    ),
    "stddev_attempt": Gauge(
        "company_statistic_daily_stddev_attempt_time",
        "Population standard deviation of attempt time over the last day",
    ),
    "upper_bound_attempt": Gauge(
        "company_statistic_daily_upper_bound_time",
        "Upper bound (mean + 3·stddev) of attempt time over the last day",
    ),
    "failure_rate": Gauge(
        "attempt_create_time_all_failure_rate_percent",
        "Percentage of failed attempts (is_created = false) over the last minute",
    ),
    "created_items": Gauge(
        "attempt_create_time_all_created_items_total",
        "Count of unique items created in the last minute, by segment",
        ["segment"],
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
