import argparse
import logging
import os
import json

from datetime import datetime
from dateutil.relativedelta import relativedelta

from dataactbroker.helpers.script_helper import validate_load_dates
from dataactcore.broker_logging import configure_logging
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactvalidator.filestreaming.csv_selection import write_stream_query
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

"""
This script is used to pull updated financial assistance records (from --date to present) for SAM.
"""


def get_award_updates_query(start_date=None, end_date=None):
    """Creates a string to run as the update query. This is needed because the mod date is a variable and therefore
    a constant cannot be created.

    Args:
        start_date: a string in the mm/dd/yyyy format of the date from which to run the SQL
        end_date: a string in the mm/dd/yyyy format of the date until which to run the SQL

    Returns:
        A string representing the SQL query to run.
    """
    filter_array = []
    if start_date:
        filter_array.append(f"updated_at >= '{start_date}'")
    if end_date:
        filter_array.append(f"updated_at < '{end_date}'")
    query_filter = " AND ".join(filter_array)

    # Query Summary:
    # Each row is the latest instance of any transaction that has been updated in the requested time period
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
                    place_of_perform_state_nam,
                    place_of_perform_country_c,
                    place_of_perform_country_n,
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
                    assistance_listing_title,
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
                        ORDER BY updated_at DESC, published_fabs_id DESC
                    ) AS row_num
                FROM published_fabs
                WHERE {query_filter}) duplicates
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
    now = datetime.now()
    parser = argparse.ArgumentParser(description="Pull")
    parser.add_argument(
        "--start_date",
        help="Specify start date in mm/dd/yyyy format to compare to mod date. Overrides --auto option.",
        nargs=1,
        type=str,
    )
    parser.add_argument(
        "--end_date",
        help="Specify end date in mm/dd/yyyy format to compare to mod date. Inclusive. " + "Overrides --auto option.",
        nargs=1,
        type=str,
    )
    parser.add_argument(
        "--auto",
        help="Polls S3 for the most recently uploaded FABS_for_SAM file, " + "and uses that as the modified date.",
        action="store_true",
    )
    args = parser.parse_args()

    metrics_json = {
        "script_name": "generate_sam_fabs_export.py",
        "start_time": str(now),
        "records_provided": 0,
        "start_date": "",
    }

    start_date, end_date = validate_load_dates(args.start_date, args.end_date, args.auto, load_type="fabs_extract")
    if end_date:
        # Adding an extra day to be inclusive
        end_date = datetime.strptime(end_date, "%m/%d/%Y") + relativedelta(days=1)
        end_date = end_date.strftime("%m/%d/%Y")
    start_log = start_date or "project start"
    end_log = end_date or "present"

    metrics_json["start_date"] = start_date

    update_query = get_award_updates_query(start_date, end_date)
    formatted_today = now.strftime("%Y%m%d")

    filename = f"FABS_for_SAM_{formatted_today}.csv"
    if args.start_date or args.end_date:
        start_string = f"_from_{datetime.strptime(start_log, '%m/%d/%Y').strftime('%Y%m%d')}" if args.start_date else ""
        end_string = f"_to_{datetime.strptime(end_log, '%m/%d/%Y').strftime('%Y%m%d')}" if args.end_date else ""
        filename = f"FABS_for_SAM{start_string}{end_string}.csv"

    local_file = os.path.join(os.getcwd(), filename)
    sess = GlobalDB.db().session

    logger.info(f"Starting SQL query of financial assistance records from {start_log} to {end_log}...")
    write_stream_query(sess, update_query, local_file, local_file, True, generate_headers=True, generate_string=False)
    logger.info("Completed SQL query, file written")

    # We only want to update the external data load date if it was an automatic run, not a specific one
    if args.auto:
        update_external_data_load_date(now, datetime.now(), "fabs_extract")

    metrics_json["duration"] = str(datetime.now() - now)

    with open("generate_sam_fabs_export_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == "__main__":
    configure_logging()
    with create_app().app_context():
        main()
