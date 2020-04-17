"""Configures a Kafka Connector for Postgres Station data"""
import json, time
import logging

import requests

import topic_check 

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")
    resp = None
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    except:
        logger.info("KAFKA connector server not ready, now waiting 30s to connect again.")
        time.sleep(30)
        resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    
    if None == resp:
        logger.info("Kafka connector server isn't ready, please mannuly restart simulation.")
        time.sleep(60)
    
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # Done: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    #logger.info("connector code not completed skipping connector creation")
    topic_prefix = "org.chicago.cta.connect-"
    topic_check.build_topic(f"{topic_prefix}{CONNECTOR_NAME}")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": 500,
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": topic_prefix,
                "poll.interval.ms": 60000
            }
        }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except:
        logging.debug(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")
    logging.debug("connector created successfully")
        


if __name__ == "__main__":
    configure_connector()
