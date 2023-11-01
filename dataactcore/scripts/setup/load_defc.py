import requests
import sys
import os
import logging
import pandas as pd

from datetime import datetime

from dataactbroker.helpers.pandas_helper import check_dataframe_diff

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DEFC

from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_defc(force_reload=False):
    """ Loads the DEFC data.

        Args:
            force_reload: boolean to determine if reload should happen whether there are differences or not
    """
    start_time = datetime.now()
    defc_file = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config', 'def_codes.csv')

    try:
        # Update file from public S3 bucket
        def_codes_url = '{}/def_codes.csv'.format(CONFIG_BROKER['usas_public_reference_url'])
        r = requests.get(def_codes_url, allow_redirects=True)
        open(defc_file, 'wb').write(r.content)
    except Exception:
        pass

    logger.info('Loading defc data')
    with create_app().app_context():
        data = pd.read_csv(defc_file, dtype=str)

        # Remove all invalid DEFCs that have been left in the file so USAS can continue to display them correctly
        data = data[data['Is Valid'] == 'true']

        data = clean_data(
            data,
            DEFC,
            {'defc': 'code', 'group_name': 'group'},
            {}
        )

        diff_found = check_dataframe_diff(data, DEFC, ['defc_id'], ['code'])

        if force_reload or diff_found:
            sess = GlobalDB.db().session
            # delete any data in the DEFC table
            sess.query(DEFC).delete()

            # insert data into table
            num = insert_dataframe(data, DEFC.__table__.name, sess.connection())
            logger.info('{} records inserted to defc'.format(num))
            sess.commit()
            update_external_data_load_date(start_time, datetime.now(), 'defc')
        else:
            logger.info('No differences found, skipping defc table reload.')


if __name__ == '__main__':
    configure_logging()
    reload = '--force' in sys.argv
    load_defc(reload)
