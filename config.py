import os

PN_DELIVERY_TOPIC = 'pushowl.entity.pn_delivery'
PN_CLICK_TOPIC = 'pushowl.entity.pn_click'

# Confluent cloud configurations
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
BROKER_URL = os.environ.get('BROKER_URL')
CONFLUENT_KAFKA_API_KEY = os.environ.get('CONFLUENT_KAFKA_API_KEY')
CONFLUENT_KAFKA_API_SECRET = os.environ.get('CONFLUENT_KAFKA_API_SECRET')
SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = os.environ.get('SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO')
