import logging
import pandas as pd
import datetime
import json
import requests

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import FundingOpportunity
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_funding_opportunity_number_data():
    """ Load funding opportunity number lookup table. """
    now = datetime.datetime.now()
    metrics_json = {
        'script_name': 'load_funding_opportunity_number.py',
        'start_time': str(now),
        'records_deleted': 0,
        'records_inserted': 0
    }

    logger.info('Loading funding opportunity number data')

    batch_size = 10000

    post_body = {
        'startRecordNum': 0,
        'oppStatuses': 'forecasted|posted|closed|archived',
        'rows': batch_size
    }

    fon_resp = requests.post('https://www.grants.gov/grantsws/rest/opportunities/search/', json=post_body).json()
    total_records = fon_resp['hitCount']
    fon_list = fon_resp['oppHits']

    while post_body['startRecordNum'] + batch_size < total_records:
        post_body['startRecordNum'] += batch_size
        fon_resp = requests.post('https://www.grants.gov/grantsws/rest/opportunities/search/', json=post_body).json()
        fon_list += fon_resp['oppHits']

    fon_data = pd.DataFrame(fon_list)
    fon_data = clean_data(
        fon_data,
        FundingOpportunity,
        {
            'number': 'funding_opportunity_number',
            'title': 'title',
            'cfdalist': 'cfda_numbers',
            'agency': 'agency_name',
            'oppstatus': 'status',
            'opendate': 'open_date',
            'closedate': 'close_date',
            'doctype': 'doc_type',
            'id': 'internal_id'
        },
        {}
    )

    sess = GlobalDB.db().session
    # delete any data in the Funding Opportunity table
    metrics_json['records_deleted'] = sess.query(FundingOpportunity).delete()

    # Restart sequence so it's always starting at 1
    sess.execute("ALTER SEQUENCE funding_opportunity_funding_opportunity_id_seq RESTART")

    num = insert_dataframe(fon_data, FundingOpportunity.__table__.name, sess.connection())
    sess.commit()

    logger.info('{} records inserted to {}'.format(num, FundingOpportunity.__table__.name))
    metrics_json['records_inserted'] = num

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('load_funding_opportunity_number_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)


if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        load_funding_opportunity_number_data()
