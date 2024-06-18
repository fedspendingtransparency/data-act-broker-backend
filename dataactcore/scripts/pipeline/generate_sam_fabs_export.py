import argparse
import re
import logging
import os
import csv
import datetime
import json

from dateutil.relativedelta import relativedelta

from dataactcore.broker_logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.models.domainModels import ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

'''
This script is used to pull updated financial assistance records (from --date to present) for SAM.
It can also run with --auto to poll the specified S3 bucket (BUCKET_NAME/BUCKET_PREFIX}) for the most
recent file that was uploaded, and use the boto3 response for --date.
'''

BUCKET_NAME = CONFIG_BROKER['sam']['extract']['bucket_name']
BUCKET_PREFIX = CONFIG_BROKER['sam']['extract']['bucket_prefix']


def get_award_updates_query(mod_date):
    """ Creates a string to run as the update query. This is needed because the mod date is a variable and therefore
        a constant cannot be created.

        Args:
            mod_date: a string in the mm/dd/yyyy format of the date from which to run the SQL

        Returns:
            A string representing the SQL query to run.
    """
    # Query Summary:
    # Each row is the latest instance of any transaction that has been updated since the specified mod_date
    update_query = f"""
        WITH updated_transactions AS (
            SELECT *
            FROM (SELECT unique_award_key,
                    afa_generated_unique,
                    fain,
                    award_modification_amendme,
                    action_date,
                    is_active,
                    sai_number,
                    awarding_agency_code,
                    awarding_agency_name,
                    awarding_sub_tier_agency_c,
                    awarding_sub_tier_agency_n,
                    awarding_office_code,
                    awarding_office_name,
                    funding_agency_code,
                    funding_agency_name,
                    funding_sub_tier_agency_co,
                    funding_sub_tier_agency_na,
                    funding_office_code,
                    funding_office_name,
                    uei,
                    ultimate_parent_uei,
                    awardee_or_recipient_legal,
                    ultimate_parent_legal_enti,
                    place_of_performance_city,
                    place_of_perfor_state_code,
                    place_of_perform_country_c,
                    place_of_performance_congr,
                    place_of_perform_county_co,
                    place_of_perform_county_na,
                    place_of_performance_zip5,
                    place_of_perform_zip_last4,
                    legal_entity_address_line1,
                    legal_entity_address_line2,
                    legal_entity_city_name,
                    legal_entity_state_code,
                    legal_entity_state_name,
                    legal_entity_zip5,
                    legal_entity_zip_last4,
                    legal_entity_congressional,
                    legal_entity_country_code,
                    legal_entity_country_name,
                    assistance_listing_number,
                    period_of_performance_star,
                    period_of_performance_curr,
                    assistance_type,
                    assistance_type_desc,
                    record_type,
                    business_types,
                    business_types_desc,
                    award_description,
                    original_loan_subsidy_cost,
                    federal_action_obligation,
                    high_comp_officer1_full_na,
                    high_comp_officer1_amount,
                    high_comp_officer2_full_na,
                    high_comp_officer2_amount,
                    high_comp_officer3_full_na,
                    high_comp_officer3_amount,
                    high_comp_officer4_full_na,
                    high_comp_officer4_amount,
                    high_comp_officer5_full_na,
                    high_comp_officer5_amount,
                    ROW_NUMBER() OVER (PARTITION BY
                        UPPER(afa_generated_unique)
                        ORDER BY updated_at DESC
                    ) AS row_num
                FROM published_fabs
                WHERE updated_at >= '{mod_date}') duplicates
            WHERE duplicates.row_num = 1),
        grouped_transaction AS (
            SELECT unique_award_key,
                MIN(cast_as_date(action_date)) AS base_obligation_date,
                MAX(updated_at) AS last_modified_date
            FROM published_fabs AS pf
            WHERE EXISTS (
                SELECT 1
                FROM updated_transactions AS updated
                WHERE updated.unique_award_key = pf.unique_award_key
            )
            GROUP BY unique_award_key)

        SELECT
            ut.afa_generated_unique,
            ut.unique_award_key,
            ut.fain AS federal_award_id,
            ut.award_modification_amendme AS modification_number,
            to_char(cast_as_date(ut.action_date), 'YYYY-MM-DD') AS action_date,
            CASE WHEN ut.is_active
                THEN 'active'
                ELSE 'inactive'
                END AS status,
            NULL AS eligibility,
            ut.sai_number,
            ut.uei,
            ut.ultimate_parent_uei AS parent_uei,
            ut.awardee_or_recipient_legal AS vendor_name,
            ut.ultimate_parent_legal_enti AS parent_company_name,
            ut.awarding_agency_code AS awarding_department_code,
            ut.awarding_agency_name AS awarding_department_name,
            ut.awarding_sub_tier_agency_c AS awarding_subtier_agency_code,
            ut.awarding_sub_tier_agency_n AS awarding_subtier_agency_name,
            ut.awarding_office_code AS awarding_office_code,
            ut.awarding_office_name AS awarding_office_name,
            ut.funding_agency_code AS funding_department_code,
            ut.funding_agency_name AS funding_department_name,
            ut.funding_sub_tier_agency_co AS funding_subtier_agency_code,
            ut.funding_sub_tier_agency_na AS funding_subtier_agency_name,
            ut.funding_office_code AS funding_office_code,
            ut.funding_office_name AS funding_office_name,
            ut.place_of_performance_city AS principal_place_city_name,
            ut.place_of_perfor_state_code AS principal_place_state_code,
            ut.place_of_perform_country_c AS principal_place_country_code,
            ut.place_of_performance_congr AS principal_place_congressional_district,
            ut.place_of_perform_county_co AS principal_place_county_code,
            ut.place_of_perform_county_na AS principal_place_county_name,
            ut.place_of_performance_zip5 AS principal_place_zip5,
            ut.place_of_perform_zip_last4 AS principal_place_zip_last4,
            ut.legal_entity_address_line1 AS recipient_street_address,
            ut.legal_entity_address_line2 AS recipient_street_address2,
            ut.legal_entity_city_name AS recipient_city,
            ut.legal_entity_state_code AS recipient_state_code,
            ut.legal_entity_state_name AS recipient_state_name,
            ut.legal_entity_zip5 AS recipient_zip5,
            ut.legal_entity_zip_last4 AS recipient_zip_last4,
            ut.legal_entity_congressional AS recipient_congressional_district,
            ut.legal_entity_country_code AS recipient_country_code,
            ut.legal_entity_country_name AS recipient_country_name,
            ut.assistance_listing_number,
            to_char(cast_as_date(ut.period_of_performance_star), 'YYYY-MM-DD') AS starting_date,
            to_char(cast_as_date(ut.period_of_performance_curr), 'YYYY-MM-DD') AS ending_date,
            ut.assistance_type,
            ut.assistance_type_desc,
            ut.record_type,
            ut.business_types,
            ut.business_types_desc AS business_types_description,
            CASE WHEN ut.assistance_type IN ('07', '08')
                THEN ut.original_loan_subsidy_cost
                ELSE ut.federal_action_obligation
                END AS obligation_amount,
            NULL AS total_fed_funding_amount,
            to_char(gt.base_obligation_date, 'YYYY-MM-DD') AS base_obligation_date,
            ut.award_description AS project_description,
            to_char(gt.last_modified_date, 'YYYY-MM-DD') AS last_modified_date,
            sr.dba_name AS dba_name,
            ut.high_comp_officer1_full_na AS top_pay_employee1_name,
            ut.high_comp_officer1_amount AS top_pay_employee1_amount,
            ut.high_comp_officer2_full_na AS top_pay_employee2_name,
            ut.high_comp_officer2_amount AS top_pay_employee2_amount,
            ut.high_comp_officer3_full_na AS top_pay_employee3_name,
            ut.high_comp_officer3_amount AS top_pay_employee3_amount,
            ut.high_comp_officer4_full_na AS top_pay_employee4_name,
            ut.high_comp_officer4_amount AS top_pay_employee4_amount,
            ut.high_comp_officer5_full_na AS top_pay_employee5_name,
            ut.high_comp_officer5_amount AS top_pay_employee5_amount
        FROM updated_transactions AS ut
        JOIN grouped_transaction AS gt
            ON gt.unique_award_key = ut.unique_award_key
        LEFT JOIN sam_recipient AS sr
            ON sr.uei = ut.uei"""
    return update_query


def main():
    now = datetime.datetime.now()
    parser = argparse.ArgumentParser(description='Pull')
    parser.add_argument('--date',
                        help='Specify modified date in mm/dd/yyyy format. Overrides --auto option.',
                        nargs=1, type=str)
    parser.add_argument('--auto',
                        help='Polls S3 for the most recently uploaded FABS_for_SAM file, '
                             + 'and uses that as the modified date.',
                        action='store_true')
    args = parser.parse_args()

    metrics_json = {
        'script_name': 'generate_sam_fabs_export.py',
        'start_time': str(now),
        'records_provided': 0,
        'start_date': ''
    }

    if args.auto:
        sess = GlobalDB.db().session
        # find yesterday and the date of the last successful generation
        yesterday = datetime.datetime.now().date() - relativedelta(days=1)
        last_update = sess.query(ExternalDataLoadDate). \
            filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT['fabs_extract']).one_or_none()
        mod_date = last_update.last_load_date_start.date() if last_update else yesterday
        mod_date = mod_date.strftime('%m/%d/%Y')
        print(mod_date)

    if args.date:
        arg_date = args.date[0]
        given_date = arg_date.split('/')
        if not re.match(r'^\d{2}$', given_date[0]) or not re.match(r'^\d{2}$', given_date[1])\
                or not re.match(r'^\d{4}$', given_date[2]):
            logger.error("Date " + arg_date + " not in proper mm/dd/yyyy format")
            return
        mod_date = arg_date

    if not mod_date:
        logger.error("Date or auto setting is required.")
        return

    metrics_json['start_date'] = mod_date

    update_query = get_award_updates_query(mod_date)
    formatted_today = now.strftime('%Y%m%d')

    local_file = os.path.join(os.getcwd(), f'FABS_for_SAM_{formatted_today}.csv')
    file_path = f'{BUCKET_PREFIX}FABS_for_SAM_{formatted_today}.csv' if CONFIG_BROKER['use_aws'] else local_file
    sess = GlobalDB.db().session

    logger.info(f"Starting SQL query of financial assistance records from {mod_date} to present...")
    write_stream_query(sess, update_query, local_file, file_path, CONFIG_BROKER['local'],
                       generate_headers=True, generate_string=False, bucket=BUCKET_NAME, set_region=False)
    logger.info('Completed SQL query, file written')
    # full_file_path = os.path.join(os.getcwd(), "sam_update.csv")
    # with open(full_file_path, 'w', newline='') as csv_file:
    #     out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    #     # write headers to file
    #     headers = list(results.keys())
    #     out_csv.writerow(headers)
    #     for row in results:
    #         metrics_json['records_provided'] += 1
    #         out_csv.writerow(row)
    # # close file
    # csv_file.close()

    # We only want to update the external data load date if it was an automatic run, not a specific one
    if args.auto:
        update_external_data_load_date(now, datetime.datetime.now(), 'fabs_extract')

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('generate_sam_fabs_export_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
