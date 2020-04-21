from configparser import ConfigParser
from pathlib import Path

from producer_server import ProducerServer
from utils import get_logger

INPUT_FILE = Path(__file__).parent / "data" / "police-department-calls-for-service.json"
CONF_FILE = "app.cfg"

logger = get_logger(__file__)


def run_kafka_server(input_file: str, config_file: str):

    server_config = ConfigParser()
    server_config.read(config_file)

    producer = ProducerServer(
        input_file=input_file,
        topic_name=server_config["kafka"].get("topic"),
        bootstrap_servers=server_config["kafka"].get("bootstrap_servers"),
        client_id=server_config["kafka"].get("client_id"),
        num_partitions=server_config["kafka"].getint("num_partitions"),
        replication_factor=server_config["kafka"].getint("replication_factor"),
    )

    return producer


def simulate_data(input_file: str, config_file: str):
    producer = run_kafka_server(input_file, config_file)

    logger.info("Creating topic...")
    producer.create_topic()
    try:
        logger.info("Start generating data...")
        producer.generate_data()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        producer.close()


if __name__ == "__main__":
    simulate_data(INPUT_FILE, CONF_FILE)
