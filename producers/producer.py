import abc
import json

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer as AvroProducerImpl

import config


class BaseProducer(abc.ABC):
    # admin client instance for all Producer instances
    admin_client = None

    def __init__(self, client_id, topic_name, num_partitions, num_replicas):
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.producer = None
        self.broker_properties = {
            'bootstrap.servers': config.BROKER_URL,
            'sasl.mechanisms': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': config.CONFLUENT_KAFKA_API_KEY,
            'sasl.password': config.CONFLUENT_KAFKA_API_SECRET,
        }

        self.producer_properties = dict(self.broker_properties, **{
            'client.id': client_id,
            'queue.buffering.max.ms': 5000,  # Accumulate 5 seconds worth of data
            'queue.buffering.max.messages': 500000,
            'batch.num.messages': 20000,  # At least queue.buffering.max.ms * messages per second / 1000
            'compression.type': 'snappy',  # https://blog.cloudflare.com/squeezing-the-firehose/
            'message.send.max.retries': 100,  # Don't want messages to be dropped
            'enable.idempotence': True
        })

        self.topic_config = {
            "compression.type": "snappy",
            "message.timestamp.type": "CreateTime"
        }

        # Create the admin client instance
        if BaseProducer.admin_client is None:
            BaseProducer.admin_client = AdminClient(self.broker_properties)

        # If the topic does not already exist, try to create it
        if not self.topic_exists():
            self.create_topic()

    def topic_exists(self):
        topic_metadata = BaseProducer.admin_client.list_topics(timeout=5)
        return self.topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        new_topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
            config=self.topic_config
        )
        topic_creation = BaseProducer.admin_client.create_topics([new_topic], request_timeout=15.0)
        for topic, f in topic_creation.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
        return topic_creation

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # Flush the producer so that all the messages are written to kafka
        self.producer.flush()

    @abc.abstractmethod
    def produce(self, payload):
        raise NotImplementedError()


class AvroProducer(BaseProducer):
    """Defines and provides common functionality amongst Producers"""

    def __init__(
            self,
            client_id,
            topic_name,
            num_partitions,
            num_replicas,
            value_schema
    ):
        """Initializes a Producer object with basic settings"""
        BaseProducer.__init__(self, client_id, topic_name, num_partitions, num_replicas)
        self.value_schema = value_schema
        self.producer = AvroProducerImpl(dict(
            self.producer_properties, **{
                'schema.registry.url': config.SCHEMA_REGISTRY_URL,
                'schema.registry.basic.auth.credentials.source': 'USER_INFO',
                'schema.registry.basic.auth.user.info': config.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO,
            }), default_value_schema=value_schema)

    def produce(self, json_payload):
        self.producer.produce(topic=self.topic_name, value=json_payload)


class JSONProducer(BaseProducer):

    def __init__(self,
                 client_id,
                 topic_name,
                 num_partitions,
                 num_replicas):
        BaseProducer.__init__(self, client_id, topic_name, num_partitions, num_replicas)
        self.producer = Producer(self.producer_properties)

    def produce(self, json_payload):
        self.producer.produce(topic=self.topic_name, value=json.dumps(json_payload).encode('utf-8'))
