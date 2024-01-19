import argparse
import re
import logging
import os
import csv
import boto3
import datetime
import json

from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

'''
This script is used to pull updated financial assistance records (from --date to present) for FSRS.
It can also run with --auto to poll the specified S3 bucket (BUCKET_NAME/BUCKET_PREFIX}) for the most
recent file that was uploaded, and use the boto3 response for --date.
'''

BUCKET_NAME = CONFIG_BROKER['data_extracts_bucket']
BUCKET_PREFIX = 'fsrs_award_extracts/'


def main():
    now = datetime.datetime.now()
    parser = argparse.ArgumentParser(description='Pull')
    parser.add_argument('--date',
                        help='Specify modified date in mm/dd/yyyy format. Overrides --auto option.',
                        nargs=1, type=str)
    parser.add_argument('--auto',
                        help='Polls S3 for the most recently uploaded FABS_for_FSRS file, '
                             + 'and uses that as the modified date.',
                        action='store_true')
    args = parser.parse_args()

    metrics_json = {
        'script_name': 'get_fsrs_updates.py',
        'start_time': str(now),
        'records_provided': 0,
        'start_date': ''
    }

    if args.auto:
        s3_resource = boto3.resource('s3', region_name='us-gov-west-1')
        extract_bucket = s3_resource.Bucket(BUCKET_NAME)
        all_fsrs_extracts = extract_bucket.objects.filter(Prefix=BUCKET_PREFIX)
        mod_date = max(all_fsrs_extracts, key=lambda k: k.last_modified).last_modified.strftime("%m/%d/%Y")

    if args.date:
        arg_date = args.date[0]
        given_date = arg_date.split('/')
        if not re.match('^\d{2}$', given_date[0]) or not re.match('^\d{2}$', given_date[1])\
                or not re.match('^\d{4}$', given_date[2]):
            logger.error("Date " + arg_date + " not in proper mm/dd/yyyy format")
            return
        mod_date = arg_date

    if not mod_date:
        logger.error("Date or auto setting is required.")
        return

    metrics_json['start_date'] = mod_date

    logger.info("Starting SQL query of financial assistance records from {} to present...".format(mod_date))
    sess = GlobalDB.db().session
    """ Query Summary:
        Each row is the *latest transaction of an award* with the transaction’s modified_date being within the past day
        and also includes summary data about the award associated with the transaction.
    """
    results = sess.execute("""
    WITH updated_transactions AS (
        SELECT unique_award_key,
            CASE WHEN action_type = 'A' THEN 1 ELSE 2 END AS action_sort
        FROM published_fabs
        WHERE updated_at >= '{mod_date}'),
    base_transaction AS (
        SELECT *
        FROM (SELECT pf.unique_award_key,
                pf.awarding_sub_tier_agency_c,
                pf.action_date,
                pf.assistance_type,
                pf.assistance_type_desc,
                pf.award_description,
                pf.business_types,
                pf.period_of_performance_star,
                ROW_NUMBER() OVER (PARTITION BY
                    pf.unique_award_key
                    ORDER BY pf.action_date, action_sort, pf.award_modification_amendme NULLS FIRST, pf.uri,
                        pf.cfda_number
                ) AS row_number
            FROM published_fabs AS pf
            JOIN updated_transactions AS ut
                ON ut.unique_award_key = pf.unique_award_key) AS duplicates
        WHERE duplicates.row_number = 1),
    latest_transaction AS (
        SELECT *
        FROM (SELECT pf.unique_award_key,
                pf.action_date,
                pf.action_type,
                pf.cfda_number,
                pf.assistance_type,
                pf.assistance_type_desc,
                pf.awardee_or_recipient_legal,
                pf.uei,
                pf.awarding_agency_code,
                pf.awarding_agency_name,
                pf.awarding_office_code,
                pf.business_funds_indicator,
                pf.business_types,
                pf.fain,
                pf.funding_office_code,
                pf.funding_opportunity_goals,
                pf.funding_opportunity_number,
                pf.funding_sub_tier_agency_co,
                pf.legal_entity_address_line1,
                pf.legal_entity_address_line2,
                pf.legal_entity_congressional,
                pf.legal_entity_country_code,
                pf.legal_entity_foreign_city,
                pf.legal_entity_foreign_posta,
                pf.legal_entity_foreign_provi,
                pf.legal_entity_zip5,
                pf.legal_entity_zip_last4,
                pf.period_of_performance_curr,
                pf.place_of_performance_congr,
                pf.place_of_perform_country_c,
                pf.place_of_performance_forei,
                pf.place_of_perfor_state_code,
                pf.place_of_performance_zip4a,
                pf.record_type,
                pf.sai_number,
                pf.ultimate_parent_uei,
                pf.uri,
                ROW_NUMBER() OVER (PARTITION BY
                    pf.unique_award_key
                    ORDER BY pf.action_date DESC, action_sort DESC, pf.award_modification_amendme DESC NULLS LAST,
                        pf.uri DESC, pf.cfda_number DESC
                ) AS row_number
            FROM published_fabs AS pf
            JOIN updated_transactions AS ut
                ON ut.unique_award_key = pf.unique_award_key) AS duplicates
        WHERE duplicates.row_number = 1),
    grouped_transaction AS (
        SELECT unique_award_key,
            MAX(pf.modified_at) AS max_modified_at,
            MAX(pf.updated_at) AS max_updated_at,
            SUM(CASE WHEN pf.is_active = True
                        THEN pf.face_value_loan_guarantee
                        ELSE 0
                        END) AS total_face_value_of_direct_loan_or_loan_guarantee,
            SUM(CASE WHEN pf.is_active = True
                        THEN pf.federal_action_obligation
                        ELSE 0
                        END) AS total_federal_action_obligation,
            SUM(CASE WHEN pf.is_active = True
                        THEN CASE WHEN pf.assistance_type IN ('07', '08')
                                THEN pf.original_loan_subsidy_cost
                                ELSE pf.federal_action_obligation
                                END
                        ELSE 0
                        END) AS total_generated_pragmatic_obligations,
            SUM(CASE WHEN pf.is_active = True
                        THEN pf.indirect_federal_sharing
                        ELSE 0
                        END) AS total_indirect_cost_federal_share_amount,
            SUM(CASE WHEN pf.is_active = True
                        THEN pf.non_federal_funding_amount
                        ELSE 0
                        END) AS total_non_federal_funding_amount,
            SUM(CASE WHEN pf.is_active = True
                        THEN pf.original_loan_subsidy_cost
                        ELSE 0
                        END) AS total_original_loan_subsidy_cost,
            CASE WHEN EXISTS (
                        SELECT 1
                        FROM published_fabs AS sub_pf
                        WHERE is_active = True
                        AND pf.unique_award_key = sub_pf.unique_award_key
                      )
                THEN 'active'
                ELSE 'inactive'
                END AS is_active
        FROM published_fabs AS pf
        WHERE EXISTS (
            SELECT 1
            FROM updated_transactions
            WHERE updated_transactions.unique_award_key = pf.unique_award_key
        )
        GROUP BY unique_award_key)

    SELECT
        gt.*,
        bt.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_code,
        bt.action_date AS base_action_date,
        bt.assistance_type AS base_assistance_type,
        bt.assistance_type_desc AS base_assistance_type_description,
        bt.award_description AS base_award_description,
        bt.business_types AS base_business_types,
        bt.period_of_performance_star AS base_period_of_performance_start_date,
        lt.action_date AS latest_action_date,
        lt.action_type AS latest_action_type,
        lt.cfda_number AS latest_assistance_listing_number,
        lt.assistance_type AS latest_assistance_type,
        lt.assistance_type_desc AS latest_assistance_type_description,
        lt.awardee_or_recipient_legal AS latest_awardee_or_recipient_legal_entity_name,
        lt.uei AS latest_awardee_or_recipient_uei,
        lt.awarding_agency_code AS latest_awarding_agency_code,
        lt.awarding_agency_name AS latest_awarding_agency_name,
        lt.awarding_office_code AS latest_awarding_office_code,
        lt.business_funds_indicator AS latest_business_funds_indicator,
        lt.business_types AS latest_business_types,
        lt.fain AS latest_fain,
        lt.funding_office_code AS latest_funding_office_code,
        lt.funding_opportunity_goals AS latest_funding_opportunity_goals_text,
        lt.funding_opportunity_number AS latest_funding_opportunity_number,
        lt.funding_sub_tier_agency_co AS latest_funding_sub_tier_agency_code,
        lt.legal_entity_address_line1 AS latest_legal_entity_address_line1,
        lt.legal_entity_address_line2 AS latest_legal_entity_address_line2,
        lt.legal_entity_congressional AS latest_legal_entity_congressional_district,
        lt.legal_entity_country_code AS latest_legal_entity_country_code,
        lt.legal_entity_foreign_city AS latest_legal_entity_foreign_city_name,
        lt.legal_entity_foreign_posta AS latest_legal_entity_foreign_postal_code,
        lt.legal_entity_foreign_provi AS latest_legal_entity_foreign_province_name,
        lt.legal_entity_zip5 AS latest_legal_entity_zip5,
        lt.legal_entity_zip_last4 AS latest_legal_entity_zip_last4,
        lt.period_of_performance_curr AS latest_period_of_performance_current_end_date,
        lt.place_of_performance_congr AS latest_primary_place_of_performance_congressional_district,
        lt.place_of_perform_country_c AS latest_primary_place_of_performance_country_code,
        lt.place_of_performance_forei AS latest_primary_place_of_performance_foreign_location_descr,
        lt.place_of_perfor_state_code AS latest_primary_place_of_performance_state_postal_code,
        lt.record_type AS latest_record_type,
        lt.sai_number AS latest_sai_number,
        lt.ultimate_parent_uei AS latest_ultimate_parent_uei,
        lt.uri AS latest_uri,
        lt.place_of_performance_zip4a AS latest_primary_place_of_performance_zip4
    FROM latest_transaction AS lt
    JOIN base_transaction AS bt
        ON lt.unique_award_key = bt.unique_award_key
    JOIN grouped_transaction AS gt
        ON gt.unique_award_key = lt.unique_award_key""".format(mod_date=mod_date))
    logger.info("Completed SQL query, starting file writing")

    full_file_path = os.path.join(os.getcwd(), "fsrs_update.csv")
    with open(full_file_path, 'w', newline='') as csv_file:
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        # write headers to file
        headers = ['unique_award_key', 'max_modified_at', 'max_updated_at',
                   'total_face_value_of_direct_loan_or_loan_guarantee', 'total_federal_action_obligation',
                   'total_generated_pragmatic_obligations', 'total_indirect_cost_federal_share_amount',
                   'total_non_federal_funding_amount', 'total_original_loan_subsidy_cost', 'is_active',
                   'awarding_sub_tier_agency_code', 'base_action_date', 'base_assistance_type',
                   'base_assistance_type_description', 'base_award_description', 'base_business_types',
                   'base_period_of_performance_start_date', 'latest_action_date', 'latest_action_type',
                   'latest_assistance_listing_number', 'latest_assistance_type', 'latest_assistance_type_description',
                   'latest_awardee_or_recipient_legal_entity_name', 'latest_awardee_or_recipient_uei',
                   'latest_awarding_agency_code', 'latest_awarding_agency_name', 'latest_awarding_office_code',
                   'latest_business_funds_indicator', 'latest_business_types', 'latest_fain',
                   'latest_funding_office_code', 'latest_funding_opportunity_goals_text',
                   'latest_funding_opportunity_number', 'latest_funding_sub_tier_agency_code',
                   'latest_legal_entity_address_line1', 'latest_legal_entity_address_line2',
                   'latest_legal_entity_congressional_district', 'latest_legal_entity_country_code',
                   'latest_legal_entity_foreign_city_name', 'latest_legal_entity_foreign_postal_code',
                   'latest_legal_entity_foreign_province_name', 'latest_legal_entity_zip5',
                   'latest_legal_entity_zip_last4', 'latest_period_of_performance_current_end_date',
                   'latest_primary_place_of_performance_congressional_district',
                   'latest_primary_place_of_performance_country_code',
                   'latest_primary_place_of_performance_foreign_location_descr',
                   'latest_primary_place_of_performance_state_postal_code', 'latest_record_type',
                   'latest_sai_number', 'latest_ultimate_parent_uei', 'latest_uri',
                   'latest_primary_place_of_performance_zip4']
        out_csv.writerow(headers)
        for row in results:
            metrics_json['records_provided'] += 1
            out_csv.writerow(row)
    # close file
    csv_file.close()

    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('get_fsrs_updates_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
