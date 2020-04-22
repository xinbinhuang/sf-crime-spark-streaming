import json
import random
import time
from typing import Callable, List

from cached_property import cached_property
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from utils import JsonSerializer, get_logger

logger = get_logger(__file__)


class ProducerServer:
    """A Kafka Producer used to simulate SF Crime data

    Attributes:
        bootstrap_servers (str): comma-separated Kafka brokers, e.g. localhost:9092,localhost:9093
        input_file (str): path to the data file for simulation
        topic_name (str): Kafka topic name
        key_serializer (Callable): function used to serialize the message key into bytes
        value_serializer (Callable): function used to serialize the message value into bytes
        num_partitions (int): number of partitions for the topic
        replication_factor (int): number of replications for the topic
        **conf: other key word arguments passed to kafka.KafkaProducer
    """

    def __init__(
        self,
        bootstrap_servers: str,
        input_file: str,
        topic_name: str,
        key_serializer: Callable = str.encode,
        value_serializer: Callable = JsonSerializer().serialize,
        num_partitions: int = 3,
        replication_factor: int = 1,
        **conf,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_file = input_file
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self.conf: dict = conf
        self.producer = KafkaProducer(
            key_serializer=key_serializer, value_serializer=value_serializer, **conf
        )

    @cached_property
    def client(self) -> KafkaAdminClient:
        """KafkaAdminClinet to manage topics and other cluster metadata"""
        bootstrap_servers: List[str] = self.bootstrap_servers.split(",")
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, client_id=self.conf.get("client_id")
        )
        return admin_client

    def create_topic(self):
        """Create Kafka topic on the brokers"""
        new_topic = NewTopic(
            name=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
        )

        try:
            resp = self.client.create_topics([new_topic], timeout_ms=10000)
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {new_topic.name}")
        else:
            for topic_name, err_code, err_msg in resp.topic_errors:
                if err_code != 0:
                    raise f"Error Code [{err_code}] when creating {topic_name}: {err_msg}"
            logger.info(f"Topic created: {topic_name}")
        finally:
            self.client.close()

    def generate_data(self):
        """Iterate the JSON data and send it to the Kafka Topic"""
        data = self.read_data()
        for record in data:
            key = record.get("crime_id")

            logger.debug(f"Message| key={key} | value={record}")
            future = self.producer.send(topic=self.topic_name, key=key, value=record)
            future.add_callback(self._on_success).add_errback(self._on_err)
            time.sleep(random.random())

    def read_data(self) -> dict:
        """Load in a JSON data file"""
        with open(self.input_file) as json_file:
            return json.load(json_file)

    def close(self):
        """Flush out all buffered messages and close down the producer gracefully"""
        self.producer.flush(timeout=10)
        self.producer.close(timeout=10)

    def _on_success(self, record_metadata):
        """Message on sucess callback"""
        logger.debug(
            f"Successful delivery - {record_metadata.topic}[{record_metadata.partition}]:{record_metadata.offset}"
        )

    def _on_err(self, exc):
        """Message on error callback"""
        logger.error(exc)
