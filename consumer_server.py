from typing import List
from configparser import ConfigParser

from kafka import KafkaConsumer

from utils import load_config, get_logger
from utils import JsonSerializer


logger = get_logger(__file__)


def run_kafka_consumer(config: ConfigParser):
    bootstrap_servers: List[str] = config["kafka"].get("bootstrap_servers").split(",")

    consumer = KafkaConsumer(
        config["kafka"].get("topic"),
        bootstrap_servers=bootstrap_servers,
        group_id=config["kafka"].get("group_id"),
        auto_offset_reset=config["kafka"].get("auto_offset_reset"),
        key_deserializer=bytes.decode,
        value_deserializer=JsonSerializer().deserialize,
    )
    return consumer


def consume_data():
    config = load_config()
    consumer = run_kafka_consumer(config)
    try:
        for message in consumer:
            logger.info(
                f"{message.topic}:{message.partition}:{message.offset} | key={message.key} value={message.value}"
            )
    except KeyboardInterrupt:
        logger.info("Stop consuming...")


if __name__ == "__main__":
    consume_data()