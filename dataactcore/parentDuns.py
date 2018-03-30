import logging
from suds.client import Client
import pandas as pd
import time
from sqlalchemy import and_, func

from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.fileE import config_valid, get_entities
from dataactcore.models.domainModels import DUNS
from dataactcore.scripts.loadDUNS import load_duns_by_row
from dataactcore.models.jobModels import FileType, Submission # noqa
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa

logger = logging.getLogger(__name__)


def sams_config_is_valid():
    """Check if config is valid and should be only run once per load. Returns client obj used to acces SAM API"""
    if config_valid():
        return Client(CONFIG_BROKER['sam']['wsdl'])
    else:
        logger.error({
            'message': "Invalid SAM config",
            'message_type': 'CoreError'
        })
        raise Exception('Invalid SAM WSDL config')


def get_parent_from_sams(client, duns_list, block_size):
    """Calls SAM API to retrieve parent DUNS data by DUNS number. Returns DUNS info as Data Frame"""
    duns_parent = [{
        'awardee_or_recipient_uniqu': suds_obj.entityIdentification.DUNS,
        'ultimate_parent_unique_ide': suds_obj.coreData.DUNSInformation.globalParentDUNS.DUNSNumber,
        'ultimate_parent_legal_enti': (suds_obj.coreData.DUNSInformation.globalParentDUNS.legalBusinessName
                                       or '').upper()
    }
        for suds_obj in get_entities(client, duns_list)
        if suds_obj.coreData.DUNSInformation.globalParentDUNS.DUNSNumber
        and suds_obj.coreData.DUNSInformation.globalParentDUNS.legalBusinessName
    ]
    logger.info("Retrieved {} out of {} duns numbers from SAM ".format(str(len(duns_parent)), str(block_size)))

    return pd.DataFrame(duns_parent)


def update_missing_parent_names(sess, updated_date=None):
    """Updates DUNS rows in batches where the parent DUNS number is provided but not the parent name.
       Uses other instances of the parent DUNS number where the name is populated to derive blank parent names.
       Updated_date argument used for daily DUNS loads so that only data updated that day is updated.
    """
    logger.info("Updating missing parent names")

    # Create a mapping of all the unique parent duns -> name mappings from the database
    parent_duns_by_number_name = {}

    distinct_parent_duns = sess.query(DUNS.ultimate_parent_unique_ide, DUNS.ultimate_parent_legal_enti)\
        .filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') != '',
                     func.coalesce(DUNS.ultimate_parent_unique_ide, '') != '')).distinct()

    for duns in distinct_parent_duns:
        if parent_duns_by_number_name.get(duns.ultimate_parent_unique_ide):
            # Do not want to deal with parent ids with multiple names
            del parent_duns_by_number_name[duns.ultimate_parent_unique_ide]

        parent_duns_by_number_name[duns.ultimate_parent_unique_ide] = duns.ultimate_parent_legal_enti

    # Query to find rows where the parent duns number is present, but there is no legal enetity name
    missing_parent_name = sess.query(DUNS).filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') == '',
                                                       func.coalesce(DUNS.ultimate_parent_unique_ide, '') != ''))

    if updated_date:
        missing_parent_name = missing_parent_name.filter(DUNS.updated_at >= updated_date)

    missing_count = missing_parent_name.count()

    batch = 0
    block_size = 10000
    batches = missing_count // block_size

    updated_count = 0

    while batch <= batches:

        start = time.time()
        logger.info("Processing row {} - {} with missing parent duns name"
                    .format(str(batch*block_size+1),
                            str(missing_count if batch == batches else (batch+1)*block_size)
                            ))

        missing_parent_name_block = missing_parent_name.order_by(DUNS.duns_id)\
            .offset(batch*block_size).limit(block_size)

        for row in missing_parent_name_block:

            if parent_duns_by_number_name.get(row.ultimate_parent_unique_ide):
                setattr(row, 'ultimate_parent_legal_enti', parent_duns_by_number_name[row.ultimate_parent_unique_ide])
                updated_count += 1

        logger.info("Updated {} rows in DUNS with the parent name in {} s".format(updated_count, time.time()-start))

        batch += 1

    sess.commit()


def get_duns_batches(client, sess, batch_start=None, batch_end=None, updated_date=None):
    """
    Updates DUNS table with parent duns and parent name information in 100 row batches.
    batch_start, batch_end arg used to run separate loads concurrently
    updated_date can specify duns rows to process at a certain updated_at date (used for daily DUNS load)
    """
    # SAMS will only return 100 records at a time
    block_size = 100
    batch = batch_start or 0
    duns_count = 0

    # Retrieve DUNS count to calculate number of batches based on how many duns there are
    duns = sess.query(DUNS)

    if updated_date:
        duns = duns.filter(DUNS.updated_at >= updated_date)

    if not batch_end:
        duns_count = duns.count()

    batches = batch_end or duns_count//block_size

    while batch <= batches:
        start_batch = time.time()

        logger.info('Beginning updating batch {}'.format(batch))

        duns_to_update = duns.order_by(DUNS.duns_id).offset(batch * block_size).limit(block_size)

        # DUNS rows that will be updated
        models = {row.awardee_or_recipient_uniqu: row for row in duns_to_update}

        duns_list = list(models.keys())

        # Gets parent duns data from SAM API
        duns_parent_df = get_parent_from_sams(client, duns_list, block_size)

        load_duns_by_row(duns_parent_df, sess, models, None, benchmarks=False)

        sess.commit()

        logger.info('Finished batch {}: Updated {} rows in {} s'.format(batch, block_size, time.time() - start_batch))

        batch += 1
