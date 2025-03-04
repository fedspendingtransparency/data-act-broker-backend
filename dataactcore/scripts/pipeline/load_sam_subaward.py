import asyncio
import argparse
import datetime
import json
import logging
import pandas as pd
from sqlalchemy import func
import sys

from dataactcore.broker_logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date, get_utc_now
from dataactcore.models.fsrs import SAMSubcontract, SAMSubgrant, Subaward
from dataactcore.scripts.pipeline.populate_subaward_table import populate_subaward_table, fix_broken_links
from dataactcore.utils.loader_utils import insert_dataframe

from dataactbroker.helpers.script_helper import (get_with_exception_hand, validate_load_dates, trim_nested_obj,
                                                 flatten_json)

from dataactvalidator.health_check import create_app


logger = logging.getLogger(__name__)

ASSISTANCE_API_URL = CONFIG_BROKER['sam']['subaward']['assistance_api_url'].format(CONFIG_BROKER['sam']['api_key'])
CONTRACT_API_URL = CONFIG_BROKER['sam']['subaward']['contract_api_url'].format(CONFIG_BROKER['sam']['api_key'])

# Rate limiting for this API is 10,000 requests/day
REQUESTS_AT_ONCE = 5
LIMIT = 1000


def load_subawards(sess, data_type, load_type='published', start_load_date=None, end_load_date=None, update_db=True,
                   metrics=None):
    """ Pull subaward data from the SAM Subaward API and update the DB.

        Args:
            sess: Current DB session.
            data_type: either "contract" or "assistance"
            load_type: either "published" (default) or "deleted"
            start_load_date: earliest reportUpdatedDate to pull from, None for beginning of time
            end_load_date: latest reportUpdatedDate to pull from, None for the present
            update_db: Boolean; update the DB tables with the new data from the API.
            metrics: an object containing information for the metrics file
        Returns:
            list of report_numbers pulled
        Raises:
            ValueError if load_type not specified
    """
    if data_type == 'assistance':
        api_url = ASSISTANCE_API_URL
    elif data_type == 'contract':
        api_url = CONTRACT_API_URL
    else:
        raise ValueError('data_type must be \'assistance\' or \'contract\'')

    if load_type not in ('published', 'deleted'):
        raise ValueError('data_type must be \'published\' or \'deleted\'')

    logger.info('Starting subaward feed: %s', api_url.replace(CONFIG_BROKER['sam']['api_key'], '[API_KEY]'))

    params = {
        'pageSize': LIMIT,
        'status': load_type.capitalize()
    }

    # Add start/end dates to the URL
    if start_load_date:
        params['fromDate'] = start_load_date
    if end_load_date:
        params['toDate'] = end_load_date

    # Retrieve the total count of expected records for this pull
    param_string = '&'.join(f'{k}={v}' for k, v in params.items())
    total_expected_records = get_with_exception_hand(f'{api_url}&{param_string}')['totalRecords']
    metrics[f'{load_type}_{data_type}_records'] = total_expected_records
    logger.info(f'{total_expected_records} {load_type} {data_type} record(s) expected')

    if total_expected_records == 0:
        logger.info('No records returned.')
        return []

    # Note: there currently are no sort params with their API, so we aren't logically guaranteed that the
    #       results in the pages will be in the same order every request. As of now from basic testing,
    #       it looks like they are sorting it on their end, so we can assume the page continuity for now.

    entries_processed = 0
    report_numbers_pulled = []
    while True:
        # Create an dataframe with all the data from the API
        new_subawards = pd.DataFrame()
        start = entries_processed
        for response_dict in pull_subawards(api_url, params, entries_processed):
            # Process the entry if it isn't an error
            for subaward in response_dict.get('data', []):
                entries_processed += 1
                if update_db:
                    new_subaward = parse_raw_subaward(subaward, data_type=data_type)
                    if new_subaward:
                        new_subawards = pd.concat([pd.DataFrame.from_dict([new_subaward]), new_subawards])
        new_subawards.reset_index(drop=True, inplace=True)
        report_numbers_pulled.extend(new_subawards['subaward_report_number'].tolist())

        if update_db and not new_subawards.empty:
            if load_type == 'published':
                store_subawards(sess, new_subawards, data_type)
            if load_type == 'deleted':
                # TODO: delete_subawards
                delete_subawards(sess, new_subawards, data_type)

        logger.info('Processed rows %s-%s', start, entries_processed)
        if entries_processed == total_expected_records:
            # Feed has finished
            break

        if entries_processed > total_expected_records:
            # We have somehow retrieved more records than existed at the beginning of the pull
            logger.error(f'Total expected records: {total_expected_records},'
                         f' Number of records retrieved: {entries_processed}')
            sys.exit(2)

    if update_db:
        sess.commit()

    return list(set(report_numbers_pulled))


def pull_subawards(api_url, params, entries_processed=0):
    """ Hit the SAM Subaward API and return the raw json responses

        Args:
            params: dict of the current params to lookup
            entries_processed: how many entries have been processed (for offset counting)

        Returns:
            list of json responses
    """
    async def _sam_subaward_async_get(entries_already_processed):
        response_list = []
        loop = asyncio.get_event_loop()
        futures = []
        for start_offset in range(REQUESTS_AT_ONCE):
            # pageNumber is 0-indexed
            params['pageNumber'] = (entries_already_processed // LIMIT) + start_offset
            param_string = '&'.join(f'{k}={v}' for k, v in params.items())
            futures.append(
                loop.run_in_executor(
                    None,
                    get_with_exception_hand,
                    f'{api_url}&{param_string}'
                )
            )
        for response in await asyncio.gather(*futures):
            response_list.append(response)
            pass
        return response_list

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_sam_subaward_async_get(entries_processed))


def parse_raw_subaward(raw_subaward_dict, data_type):
    """ Given a raw subaward object, translate it into a dictionary for the database

        Args:
            org: json dict of the raw subaward data to process

        Returns:
            dict of subaward prepped for the database (or None if it's to be skipped)
    """
    # trim incoming values
    raw_subaward_dict = trim_nested_obj(raw_subaward_dict)
    flat_subaward_dict = flatten_json(raw_subaward_dict)

    if data_type == 'assistance':
        mapping = {
            "description": "subawardDescription",
            "subaward_report_id": "subawardReportId",
            "subaward_report_number": "subawardReportNumber",
            "unique_award_key": "primeAwardKey",
            "date_submitted": "submittedDate",
            "award_number": "subAwardNumber",
            "award_amount": "subAwardAmount",  # TODO: scientific notation, rounding to cents?
            "action_date": "subAwardDate",
            "uei": "subVendorUei",
            "legal_business_name": "subVendorName",
            "parent_uei": "subParentUei",
            "parent_legal_business_name": "subParentName",
            "dba_name": "subDbaName",

            # TODO: "vendorPhysicalAddress" - is this the prime or sub?
            "legal_entity_address_line1": "vendorPhysicalAddress_streetAddress",
            "legal_entity_address_line2": "vendorPhysicalAddress_streetAddress2",
            "legal_entity_city_name": "vendorPhysicalAddress_city",
            "legal_entity_congressional": "vendorPhysicalAddress_congressionalDistrict",
            "legal_entity_state_code": "vendorPhysicalAddress_state_code",
            "legal_entity_state_name": "vendorPhysicalAddress_state_name",
            "legal_entity_country_code": "vendorPhysicalAddress_country_code",
            "legal_entity_country_name": "vendorPhysicalAddress_country_name",
            "legal_entity_zip_code": "vendorPhysicalAddress_zip",  # zip9 format

            # TODO: "placeOfPerformance" - is this the prime or sub?
            "ppop_address_line1": "placeOfPerformance_streetAddress",
            "ppop_city_name": "placeOfPerformance_city",
            "ppop_congressional_district": "placeOfPerformance_congressionalDistrict",
            "ppop_state_code": "placeOfPerformance_state_code",
            "ppop_state_name": "placeOfPerformance_state_name",
            "ppop_country_code": "placeOfPerformance_country_code",
            "ppop_country_name": "placeOfPerformance_country_name",
            "ppop_zip_code": "placeOfPerformance_zip",  # zip5 or zip9

            "high_comp_officer1_full_na": "subTopPayEmployee_0_salary",
            "high_comp_officer1_amount": "subTopPayEmployee_0_fullname",
            "high_comp_officer2_full_na": "subTopPayEmployee_1_salary",
            "high_comp_officer2_amount": "subTopPayEmployee_1_fullname",
            "high_comp_officer3_full_na": "subTopPayEmployee_2_salary",
            "high_comp_officer3_amount": "subTopPayEmployee_2_fullname",
            "high_comp_officer4_full_na": "subTopPayEmployee_3_salary",
            "high_comp_officer4_amount": "subTopPayEmployee_3_fullname",
            "high_comp_officer5_full_na": "subTopPayEmployee_4_salary",
            "high_comp_officer5_amount": "subTopPayEmployee_4_fullname",
        }
    elif data_type == 'contract':
        mapping = {
            "description": "subawardDescription",
            "subaward_report_id": "subAwardReportId",
            "subaward_report_number": "subAwardReportNumber",
            "unique_award_key": "primeContractKey",
            "date_submitted": "submittedDate",

            "contract_agency_code": "agencyId",
            "contract_idv_agency_code": "referencedIDVAgencyId",

            "award_number": "subAwardNumber",
            "award_amount": "subAwardAmount",  # TODO: scientific notation, rounding to cents?
            "action_date": "subAwardDate",
            "uei": "subEntityUei",
            "legal_business_name": "subEntityLegalBusinessName",
            "parent_uei": "subParentUei",
            "parent_legal_business_name": "subEntityParentLegalBusinessName",
            "dba_name": "subEntityDoingBusinessAsName",

            # TODO: "entityPhysicalAddress" - is this the prime or sub?
            "legal_entity_address_line1": "entityPhysicalAddress_streetAddress",
            "legal_entity_address_line2": "entityPhysicalAddress_streetAddress2",
            "legal_entity_city_name": "entityPhysicalAddress_city",
            "legal_entity_congressional": "entityPhysicalAddress_congressionalDistrict",
            "legal_entity_state_code": "entityPhysicalAddress_state_code",
            "legal_entity_state_name": "entityPhysicalAddress_state_name",
            "legal_entity_country_code": "entityPhysicalAddress_country_code",
            "legal_entity_country_name": "entityPhysicalAddress_country_name",
            "legal_entity_zip_code": "entityPhysicalAddress_zip",  # zip9

            # TODO: requesting place of performance for contracts
            "ppop_country_code": "",
            "ppop_country_name": "",
            "ppop_state_code": "",
            "ppop_state_name": "",
            "ppop_address_line1": "",
            "ppop_city_name": "",
            "ppop_zip_code": "",  # zip5 or zip9
            "ppop_congressional_district": "",

            "high_comp_officer1_full_na": "subTopPayEmployee_0_salary",
            "high_comp_officer1_amount": "subTopPayEmployee_0_fullname",
            "high_comp_officer2_full_na": "subTopPayEmployee_1_salary",
            "high_comp_officer2_amount": "subTopPayEmployee_1_fullname",
            "high_comp_officer3_full_na": "subTopPayEmployee_2_salary",
            "high_comp_officer3_amount": "subTopPayEmployee_2_fullname",
            "high_comp_officer4_full_na": "subTopPayEmployee_3_salary",
            "high_comp_officer4_amount": "subTopPayEmployee_3_fullname",
            "high_comp_officer5_full_na": "subTopPayEmployee_4_salary",
            "high_comp_officer5_amount": "subTopPayEmployee_4_fullname",
        }
    else:
        raise ValueError('data_type must be \'assistance\' or \'contract\'')

    subaward_dict = {k: (flat_subaward_dict.get(v) if v else None) for k, v in mapping.items()}

    # Business Type Codes are special - infinite list of dicts
    subaward_dict["business_types_codes"] = []
    subaward_dict["business_types_names"] = []
    for business_type in raw_subaward_dict.get('subBusinessType') or []:
        subaward_dict["business_types_codes"].append(business_type['code'])
        subaward_dict["business_types_names"].append(business_type['name'])

    return subaward_dict


def store_subawards(sess, new_subawards, data_type):
    """ Load the new subawards into the database

        Args:
            sess: database connection
            new_subawards: dataframe of the new subaward data to be stored
            data_type: either "contract" or "assistance"
    """
    new_subawards['created_at'] = get_utc_now()
    new_subawards['updated_at'] = get_utc_now()

    if data_type == 'assistance':
        sam_subaward_model = SAMSubgrant
    elif data_type == 'contract':
        sam_subaward_model = SAMSubcontract
    else:
        raise ValueError('data_type must be \'assistance\' or \'contract\'')

    # TODO: Confirm subaward uniquness and update accordingly
    new_sub_report_nums = new_subawards['subaward_report_number'].tolist()
    old_subs = sess.query(sam_subaward_model).filter(sam_subaward_model.subaward_report_number.in_(new_sub_report_nums))
    old_subs.delete(synchronize_session=False)

    insert_dataframe(new_subawards, sam_subaward_model.__table__.name, sess.connection())
    sess.commit()


def delete_subawards(sess, new_subawards, data_type):
    # TODO: delete_subawards
    pass


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Pull data from SAM Subaward Feed')
    parser.add_argument('-d', '--data_type', help='Which API (assistance, contract, or both) to load.'
                                                  ' Defaults to both.',
                        required=False, default='both', choices=['assistance', 'contract', 'both'])
    parser.add_argument('-l', '--load_type', help='Which status (published, deleted, or both) to load.'
                                                  ' Defaults to both.',
                        required=False, default='both', choices=['published', 'deleted', 'both'])
    parser.add_argument('--start_date',
                        help='Specify start date in mm/dd/yyyy format to compare to reportUpdatedDate.'
                             ' Overrides --auto option.',
                        nargs=1, type=str)
    parser.add_argument('--end_date',
                        help='Specify end date in mm/dd/yyyy format to compare to reportUpdatedDate. Inclusive. '
                             + 'Overrides --auto option.',
                        nargs=1, type=str)
    parser.add_argument('--auto', help='Pull records since the last load.', action='store_true')
    parser.add_argument('-i', '--ignore_db', help='Do not update the DB tables', action='store_true')

    with create_app().app_context():
        logger.info("Begin loading Subaward data from SAM APIs")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        metrics_json = {
            'script_name': 'load_sam_subaward.py',
            'start_time': str(now),
            'procurement_subawards': 0,
            'grant_subawards': 0
        }

        start_date, end_date = validate_load_dates(args.start_date, args.end_date, args.auto, 'subaward', '%m/%d/%Y',
                                                   '%Y-%m-%d')
        data_types = ['contract', 'assistance'] if args.data_type == 'both' else [args.data_type]
        load_types = ['published', 'deleted'] if args.load_type == 'both' else [args.load_type]

        # there may be more transaction data since we've last run, let's fix any links before importing new data
        last_updated_at = sess.query(func.max(Subaward.updated_at)).one_or_none()[0]
        if last_updated_at:
            for data_type in data_types:
                fix_broken_links(sess, data_type)

        start_ingestion_datetime = get_utc_now()
        pulled_report_nums = {}
        for data_type in data_types:
            for load_type in load_types:
                logger.info(f'Loading {load_type} SAM Subaward reports for {data_type}')
                report_nums = load_subawards(sess, data_type, load_type=load_type, start_load_date=start_date,
                                             end_load_date=end_date, update_db=not args.ignore_db, metrics=metrics_json)
                pulled_report_nums[f'{load_type}-{data_type}'] = report_nums
                logger.info(f'Loaded {load_type} SAM Subaward reports for {data_type}')

        logger.info('Populating subaward table based off new data')
        for loader_portion, report_nums in pulled_report_nums.items():
            load_type, data_type = tuple(loader_portion.split('-'))

            # For all scenarios, we're deleting the existing records. Only for additions, we're repopulating them.
            # TODO: Update subaward to store new SAM Subaward fields (or reuse internal_id?)
            logger.info(f'Deleting existing {load_type}-{data_type} records from the subaward table')
            sess.query(Subaward).filter(Subaward.internal_id.in_(report_nums)).delete(synchronize_session=False)

            if load_type != 'deleted':
                logger.info(f'Populating {load_type}-{data_type} records to the subaward table')
                populate_subaward_table(sess, data_type, min_date=start_ingestion_datetime, report_nums=report_nums)

        if args.data_type == 'both' and args.load_type == 'both':
            update_external_data_load_date(now, datetime.datetime.now(), 'subaward')
            metrics_json['duration'] = str(datetime.datetime.now() - now)

            with open('load_sam_subaward_metrics.json', 'w+') as metrics_file:
                json.dump(metrics_json, metrics_file)
