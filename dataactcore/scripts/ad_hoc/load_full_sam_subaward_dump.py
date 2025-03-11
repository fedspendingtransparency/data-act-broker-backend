import argparse
import datetime
import json
import logging
import boto3
import os
import pandas as pd
import numpy as np

from dataactcore.config import CONFIG_BROKER
from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.fsrs import SAMSubcontract, SAMSubgrant, Subaward
from dataactcore.scripts.pipeline.populate_subaward_table import populate_subaward_table
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def load_full_dump_file(sess, file_type, metrics=None):
    """ Load the full subaward dump file for the given file_type

        Args:
            sess: Current DB session.
            file_type: The file type
            metrics: an object containing information for the metrics file
    """
    file_filters = {
        'assistance': {
            'model': SAMSubgrant,
            'filename': 'Assistance',
            'mapping': {
                'primeawardkey': 'unique_award_key',
                'subvendoruei': 'uei',
                'subvendorname': 'legal_business_name',
                'subparentname': 'parent_legal_business_name',
                'subdbaname': 'dba_name',
                'vendorphysicaladdress_streetaddress': 'legal_entity_address_line1',
                'vendorphysicaladdress_streetaddress2': 'legal_entity_address_line2',
                'subawardtoppayemployeefullname1': 'high_comp_officer1_full_na',
                'subawardtoppayemployeesalary1': 'high_comp_officer1_amount',
                'subawardtoppayemployeefullname2': 'high_comp_officer2_full_na',
                'subawardtoppayemployeesalary2': 'high_comp_officer2_amount',
                'subawardtoppayemployeefullname3': 'high_comp_officer3_full_na',
                'subawardtoppayemployeesalary3': 'high_comp_officer3_amount',
                'subawardtoppayemployeefullname4': 'high_comp_officer4_full_na',
                'subawardtoppayemployeesalary4': 'high_comp_officer4_amount',
                'subawardtoppayemployeefullname5': 'high_comp_officer5_full_na',
                'subawardtoppayemployeesalary5': 'high_comp_officer5_amount'
            }
        },
        'contract': {
            'model': SAMSubcontract,
            'filename': 'Contracts',
            'mapping': {
                'agencyid': 'contract_agency_code',
                'referenceagencyid': 'contract_idv_agency_code',
                'primecontractkey': 'unique_award_key',
                'subentityuei': 'uei',
                'subentitylegalbusinessname': 'legal_business_name',
                'sub_parent_name': 'parent_legal_business_name',
                'sub_dba_name': 'dba_name',
                'vendor_physicaladdress_streetaddress': 'legal_entity_address_line1',
                'vendor_physicaladdress_streetaddress2': 'legal_entity_address_line2',
                'subcontractortoppayemployeefullname1': 'high_comp_officer1_full_na',
                'subcontractortoppayemployeesalary1': 'high_comp_officer1_amount',
                'subcontractortoppayemployeefullname2': 'high_comp_officer2_full_na',
                'subcontractortoppayemployeesalary2': 'high_comp_officer2_amount',
                'subcontractortoppayemployeefullname3': 'high_comp_officer3_full_na',
                'subcontractortoppayemployeesalary3': 'high_comp_officer3_amount',
                'subcontractortoppayemployeefullname4': 'high_comp_officer4_full_na',
                'subcontractortoppayemployeesalary4': 'high_comp_officer4_amount',
                'subcontractortoppayemployeefullname5': 'high_comp_officer5_full_na',
                'subcontractortoppayemployeesalary5': 'high_comp_officer5_amount'
            }
        }
    }

    dtypes = {field: 'string' for field in list(file_filters[file_type]['mapping'].keys())}

    filename = f'SAM_Subaward_Bulk_Import_{file_filters[file_type]['filename']}.csv'

    if CONFIG_BROKER['use_aws']:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        subaward_file = s3_client.generate_presigned_url(ClientMethod='get_object',
                                                         Params={'Bucket': CONFIG_BROKER['data_extracts_bucket'],
                                                                 'Key': f'sam_subaward_bulk_dump/{filename}'},
                                                         ExpiresIn=600)
    else:
        subaward_file = os.path.join(CONFIG_BROKER['path'], 'dataactvalidator', 'config', filename)

    # Clear out the entire old table
    sess.query(file_filters[file_type]['model']).delete(synchronize_session=False)

    chunk_size = 10000
    with pd.read_csv(subaward_file, encoding='cp1252', chunksize=chunk_size, dtype=dtypes, usecols=list(dtypes.keys())) as reader_obj:
        for chunk_df in reader_obj:
            data = clean_data(
                chunk_df,
                file_filters[file_type]['model'],
                {
                    'subawarddescription': 'description',
                    'subawardreportid': 'subaward_report_id',
                    'subawardreportnumber': 'subaward_report_number',
                    'submitteddate': 'date_submitted',
                    'subawardnumber': 'award_number',
                    'subawardamount': 'award_amount',
                    'subawarddate': 'action_date',
                    'subparentuei': 'parent_uei',
                    'subbusinesstype_code': 'business_types_codes',
                    'subbusinesstype_name': 'business_types_names',

                    'vendor_physicaladdress_city': 'legal_entity_city_name',
                    'vendor_physicaladdress_congressionaldistrict': 'legal_entity_congressional',
                    'vendor_physicaladdress_state_code': 'legal_entity_state_code',
                    'vendor_physicaladdress_state_name': 'legal_entity_state_name',
                    'vendor_physicaladdress_country_code': 'legal_entity_country_code',
                    'vendor_physicaladdress_country_name': 'legal_entity_country_name',
                    'vendor_physicaladdress_zip': 'legal_entity_zip_code',

                    'sub_place_of_performance_streetaddress': 'ppop_address_line1',
                    'sub_place_of_performance_city': 'ppop_city_name',
                    'sub_place_of_performance_congressional_district': 'ppop_congressional_district',
                    'sub_place_of_performance_state_code': 'ppop_state_code',
                    'sub_place_of_performance_state_name': 'ppop_state_name',
                    'sub_place_of_performance_country_code': 'ppop_country_code',
                    'sub_place_of_performance_country_name': 'ppop_country_name',
                    'sub_place_of_performance_zip': 'ppop_zip_code'
                } | file_filters[file_type]['mapping'],
                {}
            )

            data['business_types_codes'] = data['business_types_codes'].dropna().apply(json.loads)
            data['business_types_names'] = data['business_types_names'].dropna().apply(json.loads)
            # Clear any lingering np.nan's
            data = data.replace({np.nan: None})

            # Load new data in to the listed table
            logger.info(f'Begin inserting {len(chunk_df)} subawards')
            num_inserted = insert_dataframe(data, file_filters[file_type]['model'].__table__.name, sess.connection())
            metrics[f'{file_type}_subawards'] = num_inserted
            sess.commit()
            logger.info(f'Inserted {num_inserted} {file_type} subawards')


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Bulk load of SAM subaward data in raw tables')
    parser.add_argument('-f', '--file_type', help='Which file (assistance, contract, or both) to load.'
                                                  ' Defaults to both.',
                        required=False, default='both', choices=['assistance', 'contract', 'both'])
    parser.add_argument('-r', '--reload', help='If provided, reloads the subaward table from scratch after'
                                               ' loading the raw tables',
                        required=False, action='store_true')

    with create_app().app_context():
        logger.info('Begin loading Subaward data from SAM APIs')
        sess = GlobalDB.db().session
        args = parser.parse_args()

        metrics_json = {
            'script_name': 'load_full_sam_subaward_dump.py',
            'start_time': str(now),
            'assistance_subawards': 0,
            'contract_subawards': 0
        }

        file_types = ['contract', 'assistance'] if args.file_type == 'both' else [args.file_type]

        # Load full dump files for the specified subaward types
        for file_type in file_types:
            logger.info(f'Loading full dump of SAM Subaward reports for {file_type}')
            load_full_dump_file(sess, file_type, metrics=metrics_json)
            logger.info(f'Loaded full dump of SAM Subaward reports for {file_type}')

        # If reload argument is present, reload the subaward table
        if args.reload:
            logger.info('Reloading all subawards')
            sess.query(Subaward).delete(synchronize_session=False)
            # Picking a date from long ago to ensure all the update_ats are always way after it
            min_date = datetime.datetime.strptime('01/01/2020', '%m/%d/%Y')
            populate_subaward_table(sess, 'contract', min_date=min_date)
            populate_subaward_table(sess, 'assistance', min_date=min_date)

        metrics_json['duration'] = str(datetime.datetime.now() - now)

        with open('load_full_sam_subaward_dump_metrics.json', 'w+') as metrics_file:
            json.dump(metrics_json, metrics_file)
