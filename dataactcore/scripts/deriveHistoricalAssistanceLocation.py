import argparse
import logging
import re
from sqlalchemy import cast, Date

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CountyCode, CityCode, Zips
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

QUERY_SIZE = 10


def get_zip_data(sess, zip_code):
    """ Get the county code based on the zip passed """
    # if it isn't a valid zip format, just toss back nothing
    if not re.match('^\d{5}(-?\d{4})?$', zip_code):
        return None

    zip_data = None
    # if it's a 9 digit zip, try with both halves first
    if len(zip_code) > 5:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5], zip_last4=zip_code[-4:]).first()
    # if we didn't go into the last if statement or returned nothing from our previous search, try just 5 digit
    if not zip_data:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5]).first()

    return zip_data


def process_fabs_derivations(sess, county_by_code, data):
    for row in data:
        ppop_state = None
        le_state = None
        # fill the place of performance county code where needed/possible
        if not row.place_of_perform_county_co:
            # we only need to check place of performance code if it exists
            if row.place_of_performance_code:
                ppop_code = row.place_of_performance_code.upper()
                # if county style, get county code
                if re.match('[A-Z]{2}\*\*\d{3}', ppop_code):
                    ppop_state = ppop_code[:2]
                    row.place_of_perform_county_co = ppop_code[-3:]
                # if city style, check city code table
                elif re.match('[A-Z]{2}\d{5}', ppop_code):
                    ppop_state = ppop_code[:2]
                    city_info = sess.query(CityCode).filter_by(city_code=ppop_code[-5:], state_code=ppop_state).first()
                    # only set it if we got one
                    if city_info:
                        row.place_of_perform_county_co = city_info.county_number
            # check if we managed to fill it in and if we have a zip4
            if not row.place_of_perform_county_co and row.place_of_performance_zip4a:
                zip_code = row.place_of_performance_zip4a
                zip_data = get_zip_data(sess, zip_code)
                if zip_data:
                    row.place_of_perform_county_co = zip_data.county_number
                    ppop_state = zip_data.state_abbreviation
        # fill in place of performance county name where needed/possible
        if not row.place_of_perform_county_na and row.place_of_perform_county_co and ppop_state:
            if ppop_state in county_by_code and row.place_of_perform_county_co in county_by_code[ppop_state]:
                row.place_of_perform_county_na = county_by_code[ppop_state][row.place_of_perform_county_co]
        # fill in legal entity county code where needed/possible
        if not row.legal_entity_county_code:
            if row.record_type == 1 and row.place_of_performance_code\
                    and re.match('^[A-Z]{2}\*\*\d{3}$', row.place_of_performance_code.upper()):
                row.legal_entity_county_code = row.place_of_performance_code[-3:]
                le_state = row.place_of_performance_code[:2]
            elif row.legal_entity_zip5:
                zip_code = row.legal_entity_zip5
                # if we have a 4-digit zip to go with the 5-digit, combine them
                if row.legal_entity_zip_last4:
                    zip_code += row.legal_entity_zip_last4
                zip_data = get_zip_data(sess, zip_code)
                if zip_data:
                    row.legal_entity_county_code = zip_data.county_number
                    le_state = zip_data.state_abbreviation
        # fill in legal entity county name where needed/possible
        if not row.legal_entity_county_name and row.legal_entity_county_code and le_state:
            if le_state in county_by_code and row.legal_entity_county_code in county_by_code[le_state]:
                row.legal_entity_county_name = county_by_code[le_state][row.legal_entity_county_code]


def update_historical_fabs(sess, county_by_code, start, end):
    """ Derive county codes """
    model = PublishedAwardFinancialAssistance
    start_slice = 0
    while True:
        print("next set")
        query_result = sess.query(model).\
            filter(model.is_active.is_(True)).\
            filter(cast(model.action_date, Date) >= start).\
            filter(cast(model.action_date, Date) <= end).\
            slice(start_slice, start_slice + QUERY_SIZE).all()

        # break the loop
        if len(query_result) == 0:
            break

        # process the derivations for historical data
        process_fabs_derivations(sess, county_by_code, query_result)
        start_slice += QUERY_SIZE
    sess.commit()


def main():
    parser = argparse.ArgumentParser(description='Update county information for historical FABS and FPDS data')
    parser.add_argument('-t', '--type', help='Which data type, argument must be fpds or fabs', nargs=1, type=str,
                        required=True)
    parser.add_argument('-s', '--start', help='Start date, must be in the format YYYY/MM/DD', nargs=1, type=str,
                        required=True)
    parser.add_argument('-e', '--end', help='End date, must be in the format YYYY/MM/DD', nargs=1, type=str,
                        required=True)
    args = parser.parse_args()

    sess = GlobalDB.db().session

    data_type = args.type[0]

    county_codes = sess.query(CountyCode.county_number, CountyCode.state_code, CountyCode.county_name).all()
    county_by_name = {}
    county_by_code = {}

    for county_code in county_codes:
        state_code = county_code.state_code
        county_num = county_code.county_number
        county_name = county_code.county_name.strip()

        # insert state codes to each list if they aren't already in there
        if state_code not in county_by_name:
            county_by_name[state_code] = {}
        if state_code not in county_by_code:
            county_by_code[state_code] = {}

        if data_type == 'fpds':
            # we don't want any "(CA)" endings for FPDS, so strip those (also strip all extra whitespace)
            county_name = county_name.replace(' (CA)', '').strip().upper()

        # we want all the counties in our by-code lookup because we'd be using this table anyway for derivations
        county_by_code[state_code][county_num] = county_name

        # if the county name has only letters/spaces then we want it in our by-name lookup, the rest have the
        # potential to be different from the FPDS feed (and won't be used in FABS)
        if re.match('^[A-Z\s]+$', county_name):
            county_by_name[state_code][county_name] = county_num

    if data_type == 'fpds':
        print("fpds derivations")
    elif data_type == 'fabs':
        print("fabs derivations")
        update_historical_fabs(sess, county_by_code, args.start[0], args.end[0])
    else:
        logger.error("Type must be fpds or fabs.")


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
