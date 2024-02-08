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


def get_award_updates(mod_date):
    """ Runs the SQL to extract new award information for FSRS

        Args:
            mod_date: a string in the mm/dd/yyyy format of the date from which to run the SQL

        Returns:
            The results of the SQL query
    """
    logger.info("Starting SQL query of financial assistance records from {} to present...".format(mod_date))
    sess = GlobalDB.db().session
    # Query Summary:
    # Each row is the *latest transaction of an award* with the transaction’s modified_date being within the past day
    # and also includes summary data about the award associated with the transaction.
    results = sess.execute(f"""
        WITH updated_transactions AS (
            SELECT unique_award_key,
                afa_generated_unique,
                fain,
                award_modification_amendme,
                action_date,
                is_active,
                sai_number,
                awarding_agency_code,
                awarding_agency_name,
                uei,
                place_of_performance_city,
                place_of_perfor_state_code,
                place_of_perform_country_c,
                place_of_performance_congr,
                place_of_perform_county_co,
                place_of_perform_county_na,
                place_of_performance_zip4a,
                assistance_listing_number,
                period_of_performance_star,
                period_of_performance_curr,
                assistance_type,
                record_type,
                business_types,
                award_description,
                original_loan_subsidy_cost,
                federal_action_obligation
            FROM published_fabs
            WHERE updated_at >= '{mod_date}'),
        grouped_transaction AS (
            SELECT unique_award_key,
                MIN(cast_as_date(action_date)) AS base_obligation_date,
                MAX(updated_at) AS last_modified_date
            FROM published_fabs AS pf
            WHERE EXISTS (
                SELECT 1
                FROM published_fabs AS updated
                WHERE updated.unique_award_key = pf.unique_award_key
                    AND updated.updated_at >= '{mod_date}'
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
            ut.awarding_agency_code AS agency_code,
            ut.awarding_agency_name AS agency_name,
            ut.uei,
            ut.place_of_performance_city AS principal_place_city_name,
            ut.place_of_perfor_state_code AS principal_place_state_code,
            ut.place_of_perform_country_c AS principal_place_country_code,
            ut.place_of_performance_congr AS principal_place_congressional_district,
            ut.place_of_perform_county_co AS principal_place_county_code,
            ut.place_of_perform_county_na AS principal_place_county_name,
            ut.place_of_performance_zip4a AS principal_place_zip,
            ut.assistance_listing_number,
            to_char(cast_as_date(ut.period_of_performance_star), 'YYYY-MM-DD') AS starting_date,
            to_char(cast_as_date(ut.period_of_performance_curr), 'YYYY-MM-DD') AS ending_date,
            ut.assistance_type,
            ut.record_type,
            ut.business_types,
            CASE WHEN ut.assistance_type IN ('07', '08')
                THEN ut.original_loan_subsidy_cost
                ELSE ut.federal_action_obligation
                END AS obligation_amount,
            NULL AS total_fed_funding_amount,
            to_char(gt.base_obligation_date, 'YYYY-MM-DD') AS base_obligation_date,
            ut.award_description AS project_description,
            to_char(gt.last_modified_date, 'YYYY-MM-DD') AS last_modified_date
        FROM updated_transactions AS ut
        JOIN grouped_transaction AS gt
            ON gt.unique_award_key = ut.unique_award_key;""")
    return results


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
        if not re.match(r'^\d{2}$', given_date[0]) or not re.match(r'^\d{2}$', given_date[1])\
                or not re.match(r'^\d{4}$', given_date[2]):
            logger.error("Date " + arg_date + " not in proper mm/dd/yyyy format")
            return
        mod_date = arg_date

    if not mod_date:
        logger.error("Date or auto setting is required.")
        return

    metrics_json['start_date'] = mod_date

    results = get_award_updates(mod_date)
    logger.info("Completed SQL query, starting file writing")

    full_file_path = os.path.join(os.getcwd(), "fsrs_update.csv")
    with open(full_file_path, 'w', newline='') as csv_file:
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        # write headers to file
        headers = ['afa_generated_unique', 'unique_award_key', 'federal_award_id', 'modification_number', 'action_date',
                   'eligibility', 'sai_number', 'agency_code', 'agency_name', 'uei', 'principal_place_city_name',
                   'principal_place_state_code', 'principal_place_country_code',
                   'principal_place_congressional_district', 'principal_place_county_code',
                   'principal_place_county_name', 'principal_place_zip', 'assistance_listing_number', 'starting_date',
                   'ending_date', 'assistance_type', 'record_type', 'business_types', 'obligation_amount',
                   'total_fed_funding_amount', 'base_obligation_date', 'project_description', 'last_modified_date']
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
