"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        logger.info(f"message is {message}")
        #
        #
        # TODO: Process incoming weather messages. Set the temperature and status.
        #
        #
        #https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=avroconsumer#confluent_kafka.avro.AvroConsumer
        
        try:
            value = message.value()
            self.temperature = value.get("temperature")
            self.status = value.get("status")
        except Exception as e:
            logger.error(f"Exception when setting weather attributes from message{e}")


