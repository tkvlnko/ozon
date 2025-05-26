from prometheus_client import Counter, Gauge
import logging

CONSUL = "http://consul:8500"
LEADER_KEY = "microservice/leader"
SESSION_TTL = "30s"
RENEW_INTERVAL = 10
METRICS_PORT = 84
POLL_INTERVAL = 60

BASE = RENEW_INTERVAL  # 10
JITTER_FACTOR = 0.1  # 10%

CH_PASSWORD = "default"
CH_USER = "default"
CH_PORT = 9000


GAUGES = {
    # 1) События SAVE/UPDATE из product_load_events
    "save_count": Gauge(
        "product_load_events_save_count",
        "Count of product_load_events where event = 'SAVE'",
    ),
    "update_count": Gauge(
        "product_load_events_update_count",
        "Count of product_load_events where event = 'UPDATE'",
    ),
    # 2) Уникальные продавцы за период (company_statistic_monthly) :contentReference[oaicite:0]{index=0}
    "unique_sellers": Gauge(
        "company_statistic_monthly_unique_sellers",
        "Unique count of company_id in company_statistic_monthly over the time filter",
    ),
    # 3) Продавцы с первого раза (frst = al) :contentReference[oaicite:1]{index=1}
    "first_time_sellers": Gauge(
        "company_statistic_monthly_first_time_sellers",
        "Unique count of company_id where frst = al in company_statistic_monthly over the time filter",
    ),
    # 4) Продавцы повторно (frst = 0) :contentReference[oaicite:2]{index=2}
    "repeat_sellers": Gauge(
        "company_statistic_monthly_repeat_sellers",
        "Unique count of company_id where frst = 0 in company_statistic_monthly over the time filter",
    ),
    # 5) Среднее время создания товара за период :contentReference[oaicite:3]{index=3}
    "avg_full_time": Gauge(
        "attempt_create_time_avg_full_time",
        "Average full_time from attempt_create_time over the time filter",
    ),
    # 6) Стандартное отклонение времени создания :contentReference[oaicite:4]{index=4}
    "stddev_full_time": Gauge(
        "attempt_create_time_stddev_full_time",
        "Population standard deviation of full_time from attempt_create_time over the time filter",
    ),
    # 7) Верхняя граница (avg + 3·stddev) :contentReference[oaicite:5]{index=5}
    "upper_bound_full_time": Gauge(
        "attempt_create_time_upper_bound_full_time",
        "Upper bound (avg + 3*stddev) of full_time from attempt_create_time over the time filter",
    ),
    # 8) Уникальные товары, созданные (is_created) :contentReference[oaicite:6]{index=6}
    "items_created": Gauge(
        "attempt_create_time_items_created",
        "Unique count of item_id where is_created = 1 in attempt_create_time",
    ),
    # 9) Сумма попыток за последние сутки :contentReference[oaicite:7]{index=7}
    "total_attempts": Gauge(
        "item_change_log_stat_total_attempts_last_day",
        "Sum of originRow.2 from item_change_log_stat over the last day",
    ),
    # 10) 90-й процентиль времени загрузки :contentReference[oaicite:8]{index=8}
    "p90_load_time": Gauge(
        "attempt_create_time_load_time_90th_percentile",
        "90th percentile of cnt metric (load time) from attempt_create_time over the time filter",
    ),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
