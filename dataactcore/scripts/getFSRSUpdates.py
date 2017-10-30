import argparse
import re
import logging
import os
import csv

from dataactcore.logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-d', '--date',
                        help='Specify modified date in mm/dd/yyyy format. Defaults to 09/20/2017 if none is given.',
                        nargs=1, type=str)
    args = parser.parse_args()

    mod_date = '09/20/2017'
    # allow user to set start date
    if args.date:
        arg_date = args.date[0]
        given_date = arg_date.split('/')
        # error if it's in the wrong format somehow
        if not re.match('^\d{2}$', given_date[0]) or not re.match('^\d{2}$', given_date[1])\
                or not re.match('^\d{4}$', given_date[2]):
            logger.error("Date " + arg_date + " not in proper mm/dd/yyyy format")
            return
        mod_date = arg_date

    logger.info("Starting SQL query")
    sess = GlobalDB.db().session
    results = sess.execute("""
    WITH base_transaction AS (
    SELECT fain,
        MIN(pafa_b.action_date) as base_date,
        MIN(pafa_b.period_of_performance_star) as earliest_start,
        MAX(pafa_b.period_of_performance_curr) as latest_end,
        MAX(pafa_b.modified_at) as max_mod,
        SUM(CASE WHEN pafa_b.is_active = True
                    THEN pafa_b.federal_action_obligation
                    ELSE 0
                    END) as obligation_sum,
        CASE WHEN EXISTS (SELECT 1
                            FROM published_award_financial_assistance AS sub_pafa_b
                            WHERE is_active = True
                            AND pafa_b.fain = sub_pafa_b.fain)
            THEN True
            ELSE False
            END AS currently_active
    FROM published_award_financial_assistance AS pafa_b
    WHERE assistance_type IN ('02', '03', '04', '05')
    GROUP BY fain),
    only_base AS (SELECT pafa.*, base_date, earliest_start, latest_end, currently_active, obligation_sum
        FROM published_award_financial_assistance AS pafa
        JOIN base_transaction AS bt
            ON bt.fain = pafa.fain
            AND bt.max_mod = pafa.modified_at)
    
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
        ob.place_of_performance_city AS principal_place_cc,
        CASE WHEN UPPER(LEFT(ob.place_of_performance_code, 2)) ~ '[A-Z]{2}'
            THEN UPPER(LEFT(ob.place_of_performance_code, 2))
            ELSE NULL
            END AS principal_place_state_code,
        ob.place_of_perform_country_c AS principal_place_country_code,
        ob.place_of_performance_zip4a AS principal_place_zip,
        ob.cfda_number AS cfda_program_num,
        ob.earliest_start AS starting_date,
        ob.latest_end AS ending_date,
        ob.obligation_sum as total_fed_funding_amount,
        ob.base_date AS base_obligation_date,
        ob.award_description AS project_description,
        ob.modified_at AS last_modified_date
    FROM only_base AS ob
    WHERE modified_at >= '""" + mod_date + "'")
    logger.info("Completed SQL query, starting file writing")

    full_file_path = os.path.join(os.getcwd(), "fsrs_update.csv")
    with open(full_file_path, 'w', newline='') as csv_file:
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        # write headers to file
        headers = ['federal_award_id', 'status', 'eligibility', 'sai_number', 'agency_code', 'duns_no', 'dunsplus4',
                   'principal_place_cc', 'principal_place_state_code', 'principal_place_country_code',
                   'principal_place_zip', 'cfda_program_num', 'starting_date', 'ending_date',
                   'total_fed_funding_amount', 'base_obligation_date', 'project_description', 'last_modified_date']
        out_csv.writerow(headers)
        for row in results:
            out_csv.writerow(row)
    # close file
    csv_file.close()
    logger.info("Script complete")

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()