from configparser import ConfigParser
from pathlib import Path

from producer_server import ProducerServer
from utils import get_logger, load_config

INPUT_FILE = Path(__file__).parent / "data" / "police-department-calls-for-service.json"


logger = get_logger(__file__)


def run_kafka_producer(input_file: str, config: ConfigParser):

    producer = ProducerServer(
        input_file=input_file,
        topic_name=config["kafka"].get("topic"),
        bootstrap_servers=config["kafka"].get("bootstrap_servers"),
        client_id=config["kafka"].get("client_id"),
        num_partitions=config["kafka"].getint("num_partitions"),
        replication_factor=config["kafka"].getint("replication_factor"),
    )

    return producer


def simulate_data(input_file: str):
    config = load_config()
    producer = run_kafka_producer(input_file, config)

    logger.info("Creating topic...")
    producer.create_topic()
    try:
        logger.info("Start generating data...")
        producer.generate_data()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.close()


if __name__ == "__main__":
    simulate_data(INPUT_FILE)
