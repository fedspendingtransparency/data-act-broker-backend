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
            SELECT DISTINCT fain
            FROM published_fabs AS pf_b
            WHERE assistance_type IN ('02', '03', '04', '05')
                AND record_type != 1
                AND updated_at >= '{mod_date}'),
        grouped_values AS (
            SELECT fain,
                MIN(pf.action_date) as base_date,
                MIN(pf.period_of_performance_star) as earliest_start,
                MAX(pf.period_of_performance_curr) as latest_end,
                MAX(pf.updated_at) as max_updated,
                SUM(CASE WHEN pf.is_active = True
                            THEN pf.federal_action_obligation
                            ELSE 0
                            END) as obligation_sum,
                CASE WHEN EXISTS (SELECT 1
                                    FROM published_fabs AS sub_pf
                                    WHERE is_active = True
                                    AND pf.fain = sub_pf.fain)
                    THEN True
                    ELSE False
                    END AS currently_active
            FROM published_fabs AS pf
            WHERE EXISTS (
                SELECT 1
                FROM updated_transactions AS ut
                WHERE ut.fain = pf.fain
            )
            GROUP BY fain),
        only_base AS (
            SELECT *
            FROM (SELECT pf.*,
                    base_date,
                    earliest_start,
                    latest_end,
                    currently_active,
                    obligation_sum,
                    ROW_NUMBER() OVER (PARTITION BY
                        UPPER(pf.fain)
                        ORDER BY updated_at DESC
                    ) AS row_num
                FROM published_fabs AS pf
                JOIN grouped_values AS gv
                    ON gv.fain = pf.fain
                    AND gv.max_updated = pf.updated_at
                    AND pf.record_type != 1) duplicates
            WHERE duplicates.row_num = 1)
        
        SELECT
            ob.fain AS federal_award_id,
            CASE WHEN currently_active
                THEN 'active'
                ELSE 'inactive'
                END AS status,
            CASE WHEN CAST(ob.obligation_sum as double precision) > 25000 AND CAST(ob.base_date as DATE) > '10/01/2010'
                  THEN 'Eligible'
                ELSE 'Ineligible'
                END AS eligibility,
            ob.sai_number,
            ob.awarding_sub_tier_agency_c AS agency_code,
            ob.awardee_or_recipient_uniqu AS duns_no,
            NULL AS dunsplus4,
            ob.uei AS uei,
            ob.place_of_performance_city AS principal_place_cc,
            CASE WHEN UPPER(LEFT(ob.place_of_performance_code, 2)) ~ '[A-Z]{{2}}'
                THEN UPPER(LEFT(ob.place_of_performance_code, 2))
                ELSE NULL
                END AS principal_place_state_code,
            ob.place_of_perform_country_c AS principal_place_country_code,
            ob.place_of_performance_zip4a AS principal_place_zip,
            ob.assistance_listing_number AS cfda_program_num,
            ob.earliest_start AS starting_date,
            ob.latest_end AS ending_date,
            ob.obligation_sum as total_fed_funding_amount,
            ob.base_date AS base_obligation_date,
            ob.award_description AS project_description,
            ob.updated_at AS last_modified_date
        FROM only_base AS ob; """)
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
        headers = ['federal_award_id', 'status', 'eligibility', 'sai_number', 'agency_code', 'duns_no', 'dunsplus4',
                   'uei', 'principal_place_cc', 'principal_place_state_code', 'principal_place_country_code',
                   'principal_place_zip', 'cfda_program_num', 'starting_date', 'ending_date',
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
