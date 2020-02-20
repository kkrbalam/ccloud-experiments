import requests

import config


class KsqlQuery(object):
    def __init__(self, website_id):
        self.website_id = website_id

    @classmethod
    def query_ksql(cls, query):
        return requests.post(
            f'{config.KSQL_REST_API_ENDPOINT}/query',
            headers={
                'Content-Type': 'application/vnd.ksql.v1+json; charset=utf-8'
            },
            json={
                'ksql': query
            })

    def delivery_report(self, campaign_id):
        response = self.query_ksql(f'''
            select
                count
            from
                campaign_delivery_report
            where
                rowkey = '{campaign_id}';
        ''')
        count = None
        try:
            count = response.json()[1]['row']['columns'][0]
        except (KeyError, IndexError):
            print(f"Count not found. Response text: {response.text}")
        return count

    def click_report(self, campaign_id):
        response = self.query_ksql(f'''
                    select
                        clicked_component, count
                    from
                        campaign_delivery_report
                    where
                        rowkey = {campaign_id};
                ''')
        print(response)
