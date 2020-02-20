## Confluent Cloud Experiments

### Running the experiment
#### Setup workspace
- Install requirements.txt
`pip install requirements.txt`
- create an .env file
```
SCHEMA_REGISTRY_URL=<SCHEMA_REGISTRY_URL>
SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=<SCHEMA_REGISTRY_USERNAME>:<SCHEMA_REGISTRY_PASSWORD>

BROKER_URL=<BROKER_URL>
CONFLUENT_KAFKA_API_KEY=<CONFLUENT_KAFKA_USERNAME>
CONFLUENT_KAFKA_API_SECRET=<CONFLUENT_KAFKA_PASSWORD>
SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="<CONFLUENT_KAFKA_USERNAME>" password="<CONFLUENT_KAFKA_PASSWORD>";'
```

Replace placeholders between `<`, `>` with appropriate values from confluent cloud account

#### Publish data to a topic
- Make sure that all the environment variables are exported in the current session of the shell
- run `python -m producers.publish_pn`

#### Run ksql server
- Run by docker-compose file `docker-compose up`
- Run ksql cli by `docker exec -it ksqldb-cli ksql http://ksqldb-server:8088`
- Run queries in `ksql-queries.sql` to create streams and tables.
