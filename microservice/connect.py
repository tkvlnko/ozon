
import json
import random

from clickhouse_driver import Client
from kafka import KafkaProducer

from config import logger
from config import CH_PORT, CH_USER, CH_PASSWORD


def get_clickhouse_client(
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

def get_kafka_producer(bootstrap_servers=None):

    # Возвращает KafkaProducer для публикации JSON-месседжей в топик 'KAFKA_TOPIC'.
    # Если bootstrap_servers не переданы явно, по умолчанию берутся ['kafka:9092'].
    
    if bootstrap_servers is None:
        bootstrap_servers = ["kafka:9092"]
    logger.info(f"📥 Connecting to Kafka brokers...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            batch_size=16384,
            acks="all",
            retries=3,
            compression_type="snappy",
        )
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
