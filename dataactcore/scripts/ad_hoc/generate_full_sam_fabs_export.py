import logging
import os
import datetime
import json

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
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


FULL_DUMP_QUERY = """
    WITH grouped_transaction AS (
        SELECT unique_award_key,
            MIN(cast_as_date(action_date)) AS base_obligation_date,
            MAX(updated_at) AS last_modified_date,
            SUM(CASE WHEN pf.assistance_type IN ('07', '08')
                        THEN pf.original_loan_subsidy_cost
                        ELSE pf.federal_action_obligation
                        END) as obligation_sum
        FROM published_fabs AS pf
        WHERE is_active IS TRUE
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
        CASE WHEN CAST(gt.obligation_sum as double precision) > 25000
                    AND CAST(gt.base_obligation_date as DATE) > '10/01/2010'
                THEN 'Eligible'
                ELSE 'Ineligible'
        END AS eligibility,
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
        ut.place_of_perform_state_nam AS principal_place_state_name,
        ut.place_of_perform_country_c AS principal_place_country_code,
        ut.place_of_perform_country_n AS principal_place_country_name,
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
        ut.assistance_listing_title,
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
        SUM(CASE WHEN ut.assistance_type IN ('07', '08')
                THEN ut.original_loan_subsidy_cost
                ELSE ut.federal_action_obligation
            END) OVER (PARTITION BY ut.unique_award_key ORDER BY ut.action_date ASC) AS total_fed_funding_amount,
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
    FROM published_fabs AS ut
    JOIN grouped_transaction AS gt
        ON gt.unique_award_key = ut.unique_award_key
    LEFT JOIN sam_recipient AS sr
            ON sr.uei = ut.uei
    WHERE is_active IS TRUE
"""


def main():
    now = datetime.datetime.now()
    sess = GlobalDB.db().session

    metrics_json = {
        'script_name': 'generate_full_sam_fabs_export.py',
        'start_time': str(now)
    }
    formatted_today = now.strftime('%Y%m%d')

    local_file = os.path.join(os.getcwd(), f'FABS_for_SAM_full_{formatted_today}.csv')
    file_path = f'{BUCKET_PREFIX}/FABS_for_SAM_full_{formatted_today}.csv' if CONFIG_BROKER['use_aws'] else local_file

    logger.info('Starting SQL query of active financial assistance records and writing file')
    write_stream_query(sess, FULL_DUMP_QUERY, local_file, file_path, CONFIG_BROKER['local'],
                       generate_headers=True, generate_string=False, bucket=BUCKET_NAME, set_region=False)
    logger.info('Completed SQL query, file written')

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('generate_full_sam_fabs_export_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()