"""Configures a Kafka Sink Connector for Postgres"""
import json
import logging

import requests

from config import DATABASE_URL, DATABASE_PASSWORD, DATABASE_USER, KAFKA_CONNECT_URL

logger = logging.getLogger(__name__)

CONNECTOR_NAME = "campaign_delivery_windowed"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "true",
                "connection.url": f'jdbc:postgresql://{DATABASE_URL}:5432/postgres?user={DATABASE_USER}&password={DATABASE_PASSWORD}&ssl=false',
                "topics": "spark.aggregate.campaign_delivery_windowed_new",
                "auto.create": "true",
                "insert.mode": "upsert",
                "pk.mode": "record_value",
                "pk.fields": "source_id, start",
                "fields.whitelist": "source_id,count,start,end",
                "table.name.format": "kafka_topic"
            }
        }),
    )
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
