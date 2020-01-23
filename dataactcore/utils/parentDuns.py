import logging
import sys
import time

import pandas as pd
from sqlalchemy import and_, func

from dataactbroker.helpers.sam_wsdl_helper import config_valid, get_entities
from dataactbroker.helpers.generic_helper import get_client
from dataactcore.models.domainModels import DUNS
from dataactcore.models.lookups import DUNS_BUSINESS_TYPE_DICT

logger = logging.getLogger(__name__)


def sam_config_is_valid():
    """ Check if config is valid and should be only run once per load. Returns client obj used to acces SAM API.

        Returns:
            client object representing the SAM service
    """
    if config_valid():
        return get_client()
    else:
        logger.error({
            'message': "Invalid SAM wsdl config",
            'message_type': 'CoreError'
        })
        sys.exit(1)


def get_duns_props_from_sam(client, duns_list):
    """ Calls SAM API to retrieve DUNS data by DUNS number. Returns DUNS info as Data Frame

        Args:
            client: the SAM service client
            duns_list: list of DUNS to search

        Returns:
            dataframe representing the DUNS props
    """
    duns_props_mappings = {
        'awardee_or_recipient_uniqu': 'entityIdentification.DUNS',
        'legal_business_name': 'entityIdentification.legalBusinessName',
        'dba_name': 'entityIdentification.DBAName',
        'ultimate_parent_unique_ide': 'coreData.DUNSInformation.globalParentDUNS.DUNSNumber',
        'ultimate_parent_legal_enti': 'coreData.DUNSInformation.globalParentDUNS.legalBusinessName',
        'address_line_1': 'coreData.businessInformation.physicalAddress.addressLine1',
        'address_line_2': 'coreData.businessInformation.physicalAddress.addressLine2',
        'city': 'coreData.businessInformation.physicalAddress.city',
        'state': 'coreData.businessInformation.physicalAddress.stateOrProvince',
        'zip': 'coreData.businessInformation.physicalAddress.ZIPCode',
        'zip4': 'coreData.businessInformation.physicalAddress.ZIPCodePlus4',
        'country_code': 'coreData.businessInformation.physicalAddress.country',
        'congressional_district': 'coreData.businessInformation.physicalAddress.congressionalDistrict',
        'business_types_codes': 'coreData.generalInformation.listOfBusinessTypes',
        'executive_comp_data': 'coreData.listOfExecutiveCompensationInformation'
    }
    duns_props = []
    for suds_obj in get_entities(client, duns_list):
        duns_props_dict = {}
        for duns_props_name, duns_prop_path in duns_props_mappings.items():
            nested_obj = suds_obj
            value = None
            for nested_layer in duns_prop_path.split('.'):
                nested_obj = getattr(nested_obj, nested_layer, None)
                if not nested_obj:
                    break
                elif nested_layer == duns_prop_path.split('.')[-1]:
                    value = nested_obj
            if duns_props_name == 'business_types_codes':
                value = [business_type.code for business_type in getattr(nested_obj, 'businessType', [])]
                duns_props_dict['business_types'] = [DUNS_BUSINESS_TYPE_DICT[type] for type in value
                                                     if type in DUNS_BUSINESS_TYPE_DICT]
            if duns_props_name == 'executive_comp_data':
                for index in range(1, 6):
                    duns_props_dict['high_comp_officer{}_full_na'.format(index)] = None
                    duns_props_dict['high_comp_officer{}_amount'.format(index)] = None
                for index, exec_comp in enumerate(getattr(nested_obj, 'executiveCompensationDetail', []), start=1):
                    duns_props_dict['high_comp_officer{}_full_na'.format(index)] = exec_comp.name
                    duns_props_dict['high_comp_officer{}_amount'.format(index)] = str(exec_comp.compensation)
                continue
            duns_props_dict[duns_props_name] = value
        duns_props.append(duns_props_dict)

    return pd.DataFrame(duns_props)


def update_missing_parent_names(sess, updated_date=None):
    """ Updates DUNS rows in batches where the parent DUNS number is provided but not the parent name.
        Uses other instances of the parent DUNS number where the name is populated to derive blank parent names.
        Updated_date argument used for daily DUNS loads so that only data updated that day is updated.

        Args:
            sess: the database connection
            updated_date: the date to start importing from

        Returns:
            number of DUNS updated
    """
    logger.info("Updating missing parent names")

    # Create a mapping of all the unique parent duns -> name mappings from the database
    parent_duns_by_number_name = {}

    distinct_parent_duns = sess.query(DUNS.ultimate_parent_unique_ide, DUNS.ultimate_parent_legal_enti)\
        .filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') != '',
                     DUNS.ultimate_parent_unique_ide.isnot(None))).distinct()

    # Creating a mapping (parent_duns_by_number_name) of parent duns numbers to parent name
    for duns in distinct_parent_duns:
        if parent_duns_by_number_name.get(duns.ultimate_parent_unique_ide):
            # Do not want to deal with parent ids with multiple names
            del parent_duns_by_number_name[duns.ultimate_parent_unique_ide]

        parent_duns_by_number_name[duns.ultimate_parent_unique_ide] = duns.ultimate_parent_legal_enti

    # Query to find rows where the parent duns number is present, but there is no legal entity name
    missing_parent_name = sess.query(DUNS).filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') == '',
                                                       DUNS.ultimate_parent_unique_ide.isnot(None)))

    if updated_date:
        missing_parent_name = missing_parent_name.filter(DUNS.updated_at >= updated_date)

    missing_count = missing_parent_name.count()

    batch = 0
    block_size = 10000
    batches = missing_count // block_size
    total_updated_count = 0

    while batch <= batches:
        updated_count = 0
        start = time.time()
        batch_start = batch*block_size
        logger.info("Processing row {} - {} with missing parent duns name"
                    .format(str(batch*block_size+1),
                            str(missing_count if batch == batches else (batch+1)*block_size)
                            ))

        missing_parent_name_block = missing_parent_name.order_by(DUNS.duns_id).\
            slice(batch_start, batch_start + block_size)

        for row in missing_parent_name_block:
            if parent_duns_by_number_name.get(row.ultimate_parent_unique_ide):
                setattr(row, 'ultimate_parent_legal_enti', parent_duns_by_number_name[row.ultimate_parent_unique_ide])
                updated_count += 1

        logger.info("Updated {} rows in {} with the parent name in {} s".format(updated_count, DUNS.__name__,
                                                                                time.time()-start))
        total_updated_count += updated_count

        batch += 1

    sess.commit()
    return total_updated_count
