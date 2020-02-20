from pathlib import Path

import pandas as pd


class DataSource(object):
    # To be shared across all invocations of DataSource
    pn_data = pd.read_csv(f'{Path(__name__).parents[0]}/data/data.csv')  # DataFrame

    def campaign_report(self):
        campaign_df = self.pn_data[self.pn_data.source == 'campaign']
        return campaign_df.groupby('source_id')['source_id'].agg(
            total='count'
        ).reset_index().sort_values(by='total', ascending=False)

    def campaign_report_windowed(self):
        campaign_df = self.pn_data[self.pn_data.source == 'campaign']
