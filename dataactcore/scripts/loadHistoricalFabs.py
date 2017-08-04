import os
import re
import logging
import boto
import urllib.request
import zipfile
import numpy as np
import pandas as pd

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def parse_fabs_file(f, sess):
    logger.info("starting file " + str(f.name))

    csv_file = 'datafeeds\\' + os.path.splitext(os.path.basename(f.name))[0]
    zfile = zipfile.ZipFile(f.name)
    data = pd.read_csv(zfile.open(csv_file), dtype=str, usecols=[
        'cfda_program_num', 'sai_number', 'recipient_name', 'recipient_city_code', 'recipient_city_name',
        'recipient_county_code', 'recipient_county_name', 'recipient_zip', 'recipient_type', 'action_type',
        'agency_code', 'federal_award_id', 'federal_award_mod', 'fed_funding_amount', 'non_fed_funding_amount',
        'total_funding_amount', 'obligation_action_date', 'starting_date', 'ending_date', 'assistance_type',
        'record_type', 'correction_late_ind', 'fyq_correction', 'principal_place_code', 'principal_place_state',
        'principal_place_cc', 'principal_place_country_code', 'principal_place_zip', 'principal_place_cd',
        'cfda_program_title', 'project_description', 'duns_no', 'receip_addr1', 'receip_addr2', 'receip_addr3',
        'face_loan_guran', 'orig_sub_guran', 'recipient_cd', 'rec_flag', 'recipient_country_code', 'uri',
        'recipient_state_code', 'last_modified_date'
    ])

    clean_data = format_fabs_data(data)

    logger.info("loading {} rows".format(len(clean_data.index)))

    insert_dataframe(clean_data, PublishedAwardFinancialAssistance.__table__.name, sess.connection())
    sess.commit()


def format_fabs_data(data):
    logger.info("formatting data")

    # drop rows with null FAIN and URI
    data = data[~((data['federal_award_id'].isnull()) & (data['uri'].isnull()))].copy()

    proper_casing_cols = ['recipient_name', 'recipient_city_name', 'recipient_county_name', 'receip_addr1',
                          'receip_addr2', 'receip_addr3']
    for col in proper_casing_cols:
        data[col] = data.apply(lambda x: format_proper_casing(x, col), axis=1)

    cols_with_colons = ['action_type', 'assistance_type', 'agency_code', 'recipient_type', 'correction_late_ind']
    for col in cols_with_colons:
        data[col] = data.apply(lambda x: remove_data_after_colon(x, col), axis=1)

    # data['recipient_city_code'] = data.apply(lambda x: format_integer_code(x, 'recipient_city_code', 5), axis=1)
    data['recipient_county_code'] = data.apply(lambda x: format_integer_code(x, 'recipient_county_code', 3), axis=1)
    data['legal_entity_zip5'] = data.apply(lambda x: format_zip_five(x), axis=1)
    data['legal_entity_zip_last4'] = data.apply(lambda x: format_zip_four(x), axis=1)
    data['total_funding_amount'] = data.apply(lambda x: format_total_funding(x), axis=1)
    data['starting_date'] = data.apply(lambda x: format_date(x, 'starting_date'), axis=1)
    data['ending_date'] = data.apply(lambda x: format_date(x, 'ending_date'), axis=1)
    data['record_type'] = data.apply(lambda x: format_record_type(x), axis=1)
    data['place_of_perform_county_na'] = data.apply(lambda x: format_cc_code(x, True), axis=1)
    data['place_of_perform_city'] = data.apply(lambda x: format_cc_code(x, False), axis=1)
    data['principal_place_zip'] = data.apply(lambda x: format_full_zip(x), axis=1)
    data['principal_place_cd'] = data.apply(lambda x: format_cd(x, 'principal_place_cd'), axis=1)
    data['recipient_cd'] = data.apply(lambda x: format_cd(x, 'recipient_cd'), axis=1)
    data['is_historical'] = np.full(len(data.index), True, dtype=bool)

    # adding columns missing from historical data
    null_list = [
        'awarding_sub_tier_agency_n', 'awarding_agency_code', 'awarding_agency_name', 'awarding_office_code',
        'funding_agency_name', 'funding_agency_code', 'funding_office_code', 'funding_sub_tier_agency_co',
        'funding_sub_tier_agency_na', 'legal_entity_foreign_city', 'legal_entity_foreign_posta',
        'legal_entity_foreign_provi', 'place_of_performance_forei'
    ]
    for item in null_list:
        data[item] = None

    cdata = clean_data(
        data,
        PublishedAwardFinancialAssistance,
        {
            'obligation_action_date': 'action_date',
            'action_type': 'action_type',
            'assistance_type': 'assistance_type',
            'project_description': 'award_description',
            'recipient_name': 'awardee_or_recipient_legal',
            'duns_no': 'awardee_or_recipient_uniqu',
            'awarding_agency_code': 'awarding_agency_code',
            'awarding_agency_name': 'awarding_agency_name',
            'awarding_office_code': 'awarding_office_code',
            'agency_code': 'awarding_sub_tier_agency_c',
            'awarding_sub_tier_agency_n': 'awarding_sub_tier_agency_n',
            'federal_award_mod': 'award_modification_amendme',
            'rec_flag': 'business_funds_indicator',
            'recipient_type': 'business_types',
            'cfda_program_num': 'cfda_number',
            'cfda_program_title': 'cfda_title',
            'correction_late_ind': 'correction_late_delete_ind',
            'face_loan_guran': 'face_value_loan_guarantee',
            'federal_award_id': 'fain',
            'fed_funding_amount': 'federal_action_obligation',
            'fyq_correction': 'fiscal_year_and_quarter_co',
            'funding_agency_name': 'funding_agency_name',
            'funding_agency_code': 'funding_agency_code',
            'funding_office_code': 'funding_office_code',
            'funding_sub_tier_agency_co': 'funding_sub_tier_agency_co',
            'funding_sub_tier_agency_na': 'funding_sub_tier_agency_na',
            'is_historical': 'is_historical',
            'receip_addr1': 'legal_entity_address_line1',
            'receip_addr2': 'legal_entity_address_line2',
            'receip_addr3': 'legal_entity_address_line3',
            # 'recipient_city_code': 'legal_entity_city_code',
            'recipient_city_name': 'legal_entity_city_name',
            'recipient_cd': 'legal_entity_congressional',
            'recipient_country_code': 'legal_entity_country_code',
            'recipient_county_code': 'legal_entity_county_code',
            'recipient_county_name': 'legal_entity_county_name',
            'legal_entity_foreign_city': 'legal_entity_foreign_city',
            'legal_entity_foreign_posta': 'legal_entity_foreign_posta',
            'legal_entity_foreign_provi': 'legal_entity_foreign_provi',
            'recipient_state_code': 'legal_entity_state_code',
            'legal_entity_zip5': 'legal_entity_zip5',
            'legal_entity_zip_last4': 'legal_entity_zip_last4',
            'last_modified_date': 'modified_at',
            'non_fed_funding_amount': 'non_federal_funding_amount',
            'orig_sub_guran': 'original_loan_subsidy_cost',
            'ending_date': 'period_of_performance_curr',
            'starting_date': 'period_of_performance_star',
            'principal_place_code': 'place_of_performance_code',
            'principal_place_cd': 'place_of_performance_congr',
            'principal_place_country_code': 'place_of_perform_country_c',
            'place_of_performance_forei': 'place_of_performance_forei',
            'principal_place_zip': 'place_of_performance_zip4a',
            'place_of_perform_city': 'place_of_performance_city',
            'place_of_perform_county_na': 'place_of_perform_county_na',
            'principal_place_state': 'place_of_perform_state_nam',
            'record_type': 'record_type',
            'sai_number': 'sai_number',
            'total_funding_amount': 'total_funding_amount',
            'uri': 'uri'
        }, {
            'place_of_performance_congr': {'pad_to_length': 2, 'keep_null': True},
            'legal_entity_congressional': {'pad_to_length': 2, 'keep_null': True},
            'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}
        }
    )

    # make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as
    # NULL in the db.
    cdata = cdata.replace(np.nan, '', regex=True)
    cdata = cdata.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    # generate the afa_generated_unique field
    cdata['afa_generated_unique'] = cdata.apply(lambda x: generate_unique_string(x), axis=1)

    return cdata


def format_proper_casing(row, header):
    # if string is all caps, implement "Proper Casing"
    if str(row[header]).isupper():
        return str(row[header]).title()
    return row[header]


def remove_data_after_colon(row, header):
    # return the data before the colon in the row, or None
    if ':' in str(row[header]):
        return str(row[header]).split(':')[0]
    return None


def format_integer_code(row, header, int_length):
    # ensure row[header] is an integer of length int_length
    value = None
    try:
        value = row[header]
        value = int(value)
    except ValueError:
        value = None

    return value if len(str(row[header])) == int_length else None


def format_zip_five(row):
    # remove extra characters from recipient_zip
    full_zip = re.sub("[^0-9]", "", str(row['recipient_zip']))
    # separate first 5 digits from recipient_zip or set invalid zip to None
    return str(full_zip)[:5] if len(str(full_zip)) >= 5 else None


def format_zip_four(row):
    # remove extra characters from recipient_zip
    full_zip = re.sub("[^0-9]", "", str(row['recipient_zip']))
    # separate last 4 digits from recipient_zip or set invalid zip to None
    return str(full_zip)[5:9] if len(str(full_zip)) >= 9 else None


def format_date(row, header):
    # set 01/01/1990 to None
    return row[header] if row[header] != '01/01/1900' else None


def format_full_zip(row):
    # remove extra characters from principal_place_zip or set invalid zip to None
    full_zip = re.sub("[^0-9]", "", str(row['principal_place_zip']))
    return full_zip if len(full_zip) != 5 or len(full_zip) != 9 else None


def format_cd(row, column_name):
    # remove extra characters from row[column_name] or set invalid integer to None
    congr_district = re.sub("[^0-9]", "", str(row[column_name]))
    try:
        congr_district = int(congr_district)
        if congr_district < 0 or (congr_district > 53 and congr_district not in [98, 99]):
            congr_district = None
    except ValueError:
        congr_district = None

    return congr_district if len(str(congr_district)) > 0 else None


def format_cc_code(row, is_county):
    # if pop_code is ##*****, place_of_perform_county_na and place_of_perform_city should be None
    # if pop_code is ##**###, cc_code is placed in place_of_perform_county_na
    # if pop_code is #######, cc_code is placed in place_of_perform_city
    pop_code = str(row['principal_place_code'])
    cc_code = None
    if len(pop_code) and pop_code[3:7] != '*****':
        if (pop_code[3:4] == '**' and is_county) or (pop_code[3:4] != '**' and not is_county):
            cc_code = row['principal_place_cc']

    return cc_code


def format_record_type(row):
    # Set record_type to integer at beginning of string, otherwise None
    value = None
    try:
        if len(str(row['record_type'])) > 0:
            value = int(str(row['record_type'])[:1])
    except ValueError:
        pass

    return value


def format_total_funding(row):
    # if total_funding_amount = 0 or NaN, set it to fed_funding_amount + non_fed_funding_amount
    value = 0
    try:
        value = float(row['total_funding_amount'])
        if value == 0:
            value = float(row['fed_funding_amount'])+float(row['non_fed_funding_amount'])
    except ValueError:
        pass

    return value


def generate_unique_string(row):
    # create unique string from the awarding_sub_tier_agency_c, award_modification_amendme, fain, and uri
    astac = row['awarding_sub_tier_agency_c'] if row['awarding_sub_tier_agency_c'] is not None else '-none-'
    ama = row['award_modification_amendme'] if row['award_modification_amendme'] is not None else '-none-'
    fain = row['fain'] if row['fain'] is not None else '-none-'
    uri = row['uri'] if row['uri'] is not None else '-none-'
    return astac + ama + fain + uri


def set_active_rows(sess):
    logger.info('marking current records as active')
    sess.execute(
        "UPDATE published_award_financial_assistance AS all_pafa SET is_active=TRUE " +
        "FROM (SELECT DISTINCT pafa.published_award_financial_assistance_id " +
        "FROM published_award_financial_assistance AS pafa " +
        "INNER JOIN (SELECT max(modified_at) AS modified_at, afa_generated_unique " +
        "FROM published_award_financial_assistance GROUP BY afa_generated_unique) sub_pafa " +
        "ON pafa.modified_at = sub_pafa.modified_at AND " +
        "COALESCE(pafa.afa_generated_unique, '') = COALESCE(sub_pafa.afa_generated_unique, '') " +
        "WHERE COALESCE(pafa.correction_late_delete_ind, '') != 'D' AND " +
        "COALESCE(pafa.correction_late_delete_ind, '') != 'd' ) AS selected " +
        "WHERE all_pafa.published_award_financial_assistance_id = selected.published_award_financial_assistance_id"
    )
    sess.commit()


def main():
    sess = GlobalDB.db().session

    # delete any data in the PublishedAwardFinancialAssistance table
    logger.info('deleting PublishedAwardFinancialAssistance data')
    sess.query(PublishedAwardFinancialAssistance).delete(synchronize_session=False)
    sess.commit()

    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['archive_bucket'])
        for key in s3bucket.list():
            if re.match('^\d{4}_All_(DirectPayments|Grants|Insurance|Loans|Other)_Full_\d{8}.csv.zip', key.name):
                file_path = key.generate_url(expires_in=600)
                parse_fabs_file(urllib.request.urlopen(file_path), sess)
    else:
        base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs")
        file_list = [f for f in os.listdir(base_path)]
        for file in file_list:
            if re.match('^\d{4}_All_(DirectPayments|Grants|Insurance|Loans|Other)_Full_\d{8}.csv.zip', file):
                parse_fabs_file(open(os.path.join(base_path, file)), sess)

    set_active_rows(sess)

    logger.info("Historical FABS script complete")


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()
