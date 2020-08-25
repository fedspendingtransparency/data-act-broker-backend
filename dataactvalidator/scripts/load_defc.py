import requests
import os
import logging
import pandas as pd

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DEFC

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_defc():
    """ Loads the DEFC data. """
    defc_file = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config', 'def_codes.csv')
    try:
        # Update file from public S3 bucket
        def_codes_url = 'https://files.usaspending.gov/reference_data/def_codes.csv'
        r = requests.get(def_codes_url, allow_redirects=True)
        open(defc_file, 'wb').write(r.content)
    except:
        pass

    logger.info('Loading defc data')
    with create_app().app_context():
        data = pd.read_csv(defc_file, dtype=str)

        data = clean_data(
            data,
            DEFC,
            {'defc': 'code', 'group_name': 'group'},
            {}
        )

        sess = GlobalDB.db().session
        # delete any data in the DEFC table
        sess.query(DEFC).delete()

        # insert data into table
        num = insert_dataframe(data, DEFC.__table__.name, sess.connection())
        logger.info('{} records inserted to defc'.format(num))
        sess.commit()


if __name__ == '__main__':
    configure_logging()
    load_defc()
