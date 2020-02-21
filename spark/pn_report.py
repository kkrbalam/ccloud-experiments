# /usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 work/spark/pn_report.py
import json
import logging
import os

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

PN_DELIVERY_TOPIC = 'pushowl.entity.pn_delivery_json'
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
BROKER_URL = os.environ.get('BROKER_URL')
CONFLUENT_KAFKA_API_KEY = os.environ.get('CONFLUENT_KAFKA_API_KEY')
CONFLUENT_KAFKA_API_SECRET = os.environ.get('CONFLUENT_KAFKA_API_SECRET')
SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = os.environ.get('SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO')
KAFKA_SASL_JAAS_CONFIG = os.environ.get('SASL_JAAS_CONFIG')

logger = logging.getLogger(__name__)

schema = StructType([StructField("id", StringType()),
                     StructField("source_id", StringType()),
                     StructField("source", StringType()),
                     StructField("website_id", StringType()),
                     StructField("subscriber_id", StringType()),
                     StructField("delivered_time", StringType())])


def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    logger.info(f"Broker URL: {BROKER_URL} Topic: {PN_DELIVERY_TOPIC}")

    # More configuration options at - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    pn_df = spark.readStream \
        .format("kafka") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", PN_DELIVERY_TOPIC) \
        .option("maxOffsetsPerTrigger", 10000) \
        .option("kafka.bootstrap.servers", BROKER_URL) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
        .load()

    # Extract JSON keys as columns of service_table data frame
    pn_data_df = pn_df \
        .withColumn('data', psf.from_json(psf.col("value").cast('string'), schema)).select('data.*') \
        .withColumn('delivered_time', psf.to_timestamp('delivered_time', 'yyyy-MM-dd HH:mm:ss'))
    pn_data_df.printSchema()

    campaign_report_df = pn_data_df.filter(pn_data_df.source == 'campaign').groupby(['source_id']).count()
    campaign_report_df.printSchema()

    campaign_report_df \
        .selectExpr('source_id as key', 'count as value') \
        .selectExpr('CAST(key as STRING)', 'CAST(value AS STRING)') \
        .writeStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", BROKER_URL) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
        .option("topic", "spark.aggregate.campaign_delivery") \
        .option("checkpointLocation", '/tmp/campaign_report_checkpoint/') \
        .outputMode('update') \
        .start()

    # Windowed aggregation
    windowed_report_df = pn_data_df.filter(pn_data_df.source == 'campaign') \
        .withWatermark("delivered_time", "10 days") \
        .groupBy('source_id', psf.window('delivered_time', '15 minute')) \
        .count()
    windowed_report_df.printSchema()

    def append_schema_to_value(json_val):
        return json.dumps({
            "schema": {
                "type": "struct",
                "fields": [{
                    "type": "int64",
                    "optional": False,
                    "field": "source_id"
                }, {
                    "type": "int32",
                    "optional": False,
                    "field": "count"
                }, {
                    "type": "int64",
                    "optional": False,
                    "field": "start"
                }, {
                    "type": "int64",
                    "optional": False,
                    "field": "end"
                }],
                "optional": False,
                "name": "kafka_table"
            },
            "payload": json.loads(json_val)
        })

    append_schema_to_value_udf = psf.udf(append_schema_to_value, StringType())

    # Not giving source_id as key here since we don't want to allocate partitions based on campaign_id or else
    # it will lead to hot spots. Keep key as Null, and that way it will be evenly distributed across all partitions.
    query = windowed_report_df \
        .selectExpr('CAST(source_id as INT)', 'count', 'window') \
        .withColumn('row_value', psf.to_json(psf.struct([
            'count',
            'source_id',
            psf.unix_timestamp(windowed_report_df.window.start).alias('start'),
            psf.unix_timestamp(windowed_report_df.window.end).alias('end')]))) \
        .withColumn('value', append_schema_to_value_udf('row_value')) \
        .selectExpr('CAST(value as STRING)') \
        .writeStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", BROKER_URL) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", KAFKA_SASL_JAAS_CONFIG) \
        .option("topic", "spark.aggregate.campaign_delivery_windowed") \
        .option("checkpointLocation", '/tmp/campaign_report_windowed_checkpoint/') \
        .outputMode('update') \
        .start()

    query.awaitTermination()


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("PNReport") \
    .getOrCreate()

logger.info("Spark started")

run_spark_job(spark)

spark.stop()
