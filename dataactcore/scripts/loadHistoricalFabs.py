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
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)

def parse_fabs_file(f, sess):
    logger.info("starting file " + str(f.name))

    base = os.path.basename(f.name)
    data_directory = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs/")
    base_name = os.path.splitext(base)[0]
    archive = '.'.join([base_name, 'zip'])
    full_path = ''.join([data_directory, archive])
    csv_file = 'datafeeds\\' + base_name

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
        'recipient_state_code'
    ])

    data = format_fabs_data(data)

    # insert to db
    logger.info("inserting "+str(len(data.index))+" rows")
    table_name = PublishedAwardFinancialAssistance.__table__.name
    num = insert_dataframe(data, table_name, sess.connection())
    sess.commit()

def format_fabs_data(data):
    # NOTE: commented out lines are due to the PublishedAwardFinancialAssistance model being unfinished
    
    # data['recipient_city_code'] = data.apply(lambda x: format_integer_code(x, 'recipient_city_code', 5), axis=1)
    # data['recipient_county_code'] = data.apply(lambda x: format_integer_code(x, 'recipient_county_code', 3), axis=1)
    data['legal_entity_zip5'] = data.apply(lambda x: format_zip_five(x), axis=1)
    data['legal_entity_zip_last4'] = data.apply(lambda x: format_zip_four(x), axis=1)
    data['ending_date'] = data.apply(lambda x: format_date(x, 'ending_date'), axis=1)
    data['starting_date'] = data.apply(lambda x: format_date(x, 'starting_date'), axis=1)
    data['principal_place_zip'] = data.apply(lambda x: format_full_zip(x), axis=1)
    data['principal_place_cd'] = data.apply(lambda x: format_pop_congr(x), axis=1)
    # data['place_of_perform_city'] = data.apply(lambda x: format_cc_code(x, False), axis=1) 
    # data['place_of_perform_county_na'] = data.apply(lambda x: format_cc_code(x, True), axis=1)
    data['record_type'] = data.apply(lambda x: format_record_type(x), axis=1)
    data['total_funding_amount'] = data.apply(lambda x: format_total_funding(x), axis=1)
    data['is_historical'] = np.full(len(data.index), True, dtype=bool)

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
            'agency_code': 'awarding_sub_tier_agency_c',
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
            'receip_addr1': 'legal_entity_address_line1',
            'receip_addr2': 'legal_entity_address_line2',
            'receip_addr3': 'legal_entity_address_line3',
            # 'recipient_city_code': 'legal_entity_city_code',
            # 'recipient_city_name': 'legal_entity_city_name',
            'recipient_cd': 'legal_entity_congressional',
            'recipient_country_code': 'legal_entity_country_code',
            # 'recipient_county_code': 'legal_entity_county_code',
            # 'recipient_county_name': 'legal_entity_county_name',
            # 'recipient_state_code': 'legal_entity_state_code',
            'legal_entity_zip5': 'legal_entity_zip5',
            'legal_entity_zip_last4': 'legal_entity_zip_last4',
            'non_fed_funding_amount': 'non_federal_funding_amount',
            'orig_sub_guran': 'original_loan_subsidy_cost',
            'ending_date': 'period_of_performance_curr',
            'starting_date': 'period_of_performance_star',
            'principal_place_code': 'place_of_performance_code',
            'principal_place_cd': 'place_of_performance_congr',
            'principal_place_zip': 'place_of_performance_zip4a',
            # 'place_of_perform_city': 'place_of_perform_city',
            'principal_place_country_code': 'place_of_perform_country_c',
            # 'place_of_perform_county_na': 'place_of_perform_county_na',
            # 'principal_place_state': 'place_of_perform_state_nam',
            'record_type': 'record_type',
            'sai_number': 'sai_number',
            'total_funding_amount': 'total_funding_amount',
            'uri': 'uri',
            'is_historical': 'is_historical'
        }, {
            'place_of_performance_congr': {'pad_to_length': 2, 'keep_null': True},
            'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}
        }
    )

    # Make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as 
    # NULL in the db.
    cdata = cdata.replace(np.nan, '', regex=True)
    cdata = cdata.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    # drop rows with duplicate UniqueConstraints
    length = len(cdata.index)

    cdata = cdata[(~cdata.duplicated(subset=['awarding_sub_tier_agency_c', 'award_modification_amendme', 'fain', \
            'uri'], keep='first')) | (cdata['awarding_sub_tier_agency_c'].isnull()) | \
            (cdata['award_modification_amendme'].isnull()) | (cdata['fain'].isnull()) | (cdata['uri'].isnull())]
    if len(cdata.index) < length:
        logger.info('file contained '+str(length-len(cdata.index))+' duplicate(s)')

    return cdata

def format_integer_code(row, header, int_length):
    # row[header] is an integer of length int_length
    value = None
    try:
        value = row[header]
        value = int(value)
    except ValueError:
        pass

    return value if len(str(row[header]))==int_length else None

def format_zip_five(row):
    # separate first 5 digits from recipient_zip or set invalid zip to None
    return str(row['recipient_zip'])[:5] if len(str(row['recipient_zip']))>=5 else None

def format_zip_four(row):
    # separate last 4 digits from recipient_zip or set invalid zip to None
    return str(row['recipient_zip'])[5:9] if len(str(row['recipient_zip']))>=9 else None

def format_date(row, header):
    # set 01/01/1990 to None
    return row[header] if row[header]!='01/01/1900' else None

def format_full_zip(row):
    # remove extra characters from principal_place_zip or set invalid zip to None
    full_zip = re.sub("[^0-9]", "", str(row['principal_place_zip']))
    return full_zip if len(full_zip)!=5 or len(full_zip)!=9 else None

def format_pop_congr(row):
    # remove extra characters from principal_place_cd or set invalid integer to None
    pop_congr = re.sub("[^0-9]", "", str(row['principal_place_cd']))
    try:
        pop_congr = int(pop_congr)
        if pop_congr > 53 or pop_congr < 0:
            pop_congr = None
    except ValueError:
        pass

    return pop_congr if len(str(pop_congr))>0 else None

def format_cc_code(row, is_county):
    # if pop_code is ##*****, place_of_perform_county_na and place_of_perform_city should be None
    # if pop_code is ##**###, cc_code is placed in place_of_perform_county_na
    # if pop_code is #######, cc_code is placed in place_of_perform_city
    pop_code = str(row['principal_place_code'])
    cc_code = None
    if len(pop_code) and pop_code[3:7]!='*****':
        if (pop_code[3:4]=='**' and is_county) or (pop_code[3:4]!='**' and is_county==False):
            cc_code = row['principal_place_cc']

    return cc_code

def format_record_type(row):
    # Set record_type to integer at beginning of string, otherwise None
    value = None
    try:
        if len(str(row['record_type']))>0:
            value = int(str(row['record_type'])[:1])
    except ValueError:
        pass

    return value

def format_total_funding(row):
    # if total_funding_amount = 0 or nan, set it to fed_funding_amount + non_fed_funding_amount
    value = 0
    try:
        value = float(row['total_funding_amount'])
    except ValueError:
        pass

    if value==0:
        try: 
            value = float(row['fed_funding_amount'])+float(row['non_fed_funding_amount'])
        except ValueError:
            pass

    return value

def main():
    sess = GlobalDB.db().session

    # delete previously loaded historical data
    # logger.info('deleting previous historical data')
    # historical_data = sess.query(PublishedAwardFinancialAssistance).filter_by(is_historical=True)
    # [sess.delete(elem) for elem in historical_data]

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

    logger.info("Historical FABS script complete")

if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()