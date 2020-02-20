import pandas as pd

import config
from consumers.ksql_query import KsqlQuery

ksql_query = KsqlQuery(None)

for pn_data in pd.read_csv(config.DATA_FILE_PATH, chunksize=5000):

    pn_data.dropna(subset=['delivered_time'], inplace=True)

    pn_data = pn_data[pn_data.source == 'campaign']

    pn_data = pn_data[['source_id']].groupby(['source_id']).size().reset_index(name='count')

    for index, count_data in pn_data.iterrows():
        count = ksql_query.delivery_report(int(count_data['source_id']))
        if count is not None and count < count_data['count']:
            print(f"Need auditing for source_id: {count_data['source_id']}")
