"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.info("creating or updating kafka connect connector...")
    print(f"creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.info("connector already created skipping recreation")
        print(f"connector already created skipping recreation")
        return

    logging.info("about to create connector")
    print(f"about to create connector")
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,  
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                    "batch.max.rows": "5000",
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "table.whitelist": "stations", 
                    "mode": "incrementing", 
                    "incrementing.column.name": "stop_id",  
                    "topic.prefix": "connect-",
                    "poll.interval.ms": "10000",
                },
            }
        ),
    )
     # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        print(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully.")

if __name__ == "__main__":
    configure_connector()
