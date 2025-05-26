from clickhouse_driver import Client
import random
from config import logger
from config import CH_PORT, CH_USER, CH_PASSWORD


def get_client(
    chosen_shard=random.choice(
        ["clickhouse-1", "clickhouse-2", "clickhouse-3", "clickhouse-4"]
    )
):
    logger.info(f"Connecting to ClickHouse shard: {chosen_shard}")
    client = Client(
        host=chosen_shard,
        port=CH_PORT,
        database="item_upload",
        user=CH_USER,
        password=CH_PASSWORD,
    )
    return client
