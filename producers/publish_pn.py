from pathlib import Path

import pandas as pd
from confluent_kafka import avro

import config
from producers.producer import JSONProducer

pn_delivery_avro_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/pn_delivery_value.json")

pn_delivered_producer = JSONProducer(
    client_id='pn_delivery_producer',
    topic_name=config.PN_DELIVERY_TOPIC,
    num_partitions=6,
    num_replicas=3
)

# Since we have to aggregate windowed counts for clicked and delivered notifications, it is important that
# data is published in right order. If events are not in right order, events might get dropped.
website_df = pd.read_csv(f'{Path(__name__).parents[0]}/data/data.csv')  # DataFrame

###########################################################################################
# Step 1: Clean Data
###########################################################################################

# source ID can not be empty
website_df.dropna(subset=['source_id'], inplace=True)

# convert data to proper dtypes
website_df = website_df.astype({'id': 'int64', 'subscriber_id': 'int64', 'source_id': 'int64', 'website_id': 'int64'})

# clicked component can not be na
website_df.fillna(value={'clicked_component': ''}, inplace=True)

###########################################################################################
# Step 2: publish push notifications which have a delivery time to PN_DELIVERY_TOPIC
###########################################################################################

# Chunk it by website_id in order to make it more manageable

# Delivered PNs
delivered_pns = website_df[website_df.delivered_time.notnull()]

# Sort by delivered_time so that events are ordered
delivered_pns.sort_values(by='delivered_time', inplace=True)

count = 0
for index, pn in delivered_pns.iterrows():
    try:
        pn_delivered_producer.produce({
            'id': pn.id,
            'source': pn.source,
            'subscriber_id': pn.subscriber_id,
            'source_id': pn.source_id,
            'delivered_time': pd.to_datetime(pn.delivered_time).strftime('%Y-%m-%d %H:%M:%S.%f%z'),
            'website_id': pn.website_id
        })

        # Sleep for some time after every 10000th row so that buffer does not overflow
        count += 1
        if count % 1000 == 0:
            print(f"Published {count} rows len_producer: {len(pn_delivered_producer.producer)}")

            # It's important to call poll at some regular interval or else the local
            # buffer will get full
            _ = pn_delivered_producer.producer.poll(1)
    except (TypeError, ValueError) as e:
        print(f"TypeError {e} in row {pn}")
pn_delivered_producer.close()
