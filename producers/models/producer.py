"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
CLIENT_ID = "CTA_CLIENT"
LINGER_MS = 1000
COMPRESSION_TYPE = "lz4"
BATCH_NUM_MESSAGES = 100

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = (
            { 
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": SCHEMA_REGISTRY_URL
            }
        )

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=newtopic#avroproducer-legacy

        self.producer = AvroProducer(
            self.broker_properties, 
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )




    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info(f"Creating topic {self.topic_name}")

        client = AdminClient({"bootstrap.servers": BROKER_URL})
        topic = NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)
    
        futures = client.create_topics([topic])

        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                logger.info(f"Exception occurred: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("producer close begin")
        #https://knowledge.udacity.com/questions/74248
        if self.producer is not None:
            self.producer.flush()
        logger.info("producer close complete")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
