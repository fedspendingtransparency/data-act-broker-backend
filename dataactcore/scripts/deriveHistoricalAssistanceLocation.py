import argparse
import logging
import re
from sqlalchemy import cast, Date

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CountryCode, States, CountyCode, CityCode, Zips
from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

QUERY_SIZE = 1000
country_code_map = {'USA': 'US', 'ASM': 'AS', 'GUM': 'GU', 'MNP': 'MP', 'PRI': 'PR', 'VIR': 'VI', 'FSM': 'FM',
                    'MHL': 'MH', 'PLW': 'PW', 'XBK': 'UM', 'XHO': 'UM', 'XJV': 'UM', 'XJA': 'UM', 'XKR': 'UM',
                    'XPL': 'UM', 'XMW': 'UM', 'XWK': 'UM'}


def valid_zip(zip_code):
    if not re.match('^\d{5}(-?\d{4})?$', zip_code):
        return False
    return True


def get_zip_data(sess, zip_code):
    """ Get the county code based on the zip passed """
    # if it isn't a valid zip format or someone passed None, just toss back nothing
    if not zip_code or not valid_zip(zip_code):
        return None

    zip_data = None
    # if it's a 9 digit zip, try with both halves first
    if len(zip_code) > 5:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5], zip_last4=zip_code[-4:]).first()
    # if we didn't go into the last if statement or returned nothing from our previous search, try just 5 digit
    if not zip_data:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5]).first()

    return zip_data


def clean_stored_float(clean_string, fill_amount):
    """ Remove the . from the given string and fill it (with zeroes) to the length we want """
    clean_string = re.sub('\..+', '', clean_string).zfill(fill_amount)
    return clean_string


def us_ppop(ppop, state_by_code, state_by_fips):
    """ Determines if the ppop is in a valid format to be in the US """
    # return false if it's null or not 7 digits long
    if not ppop or len(ppop) != 7:
        return False

    ppop = ppop.upper()
    if ppop[:2] in state_by_code or ppop[:2] in state_by_fips:
        return True

    return False


def split_zip(zip_code):
    """ split the zip code into 5 and 4 digit codes """
    if not valid_zip(zip_code):
        return None, None

    if len(zip_code) == 5:
        return zip_code[:5], None

    return zip_code[:5], zip_code[-4:]


def fix_fabs_le_country(row, country_list, state_by_code):
    """ Update legal entity country code/name """
    # replace legal entity country codes from US territories with USA, move them into the state slot
    if row.legal_entity_country_code.upper() in country_code_map and row.legal_entity_country_code.upper() != 'USA':
        row.legal_entity_state_code = country_code_map[row.legal_entity_country_code.upper()]
        # only add the description if it's in our list
        if row.legal_entity_state_code in state_by_code:
            row.legal_entity_state_name = state_by_code[row.legal_entity_state_code]
        row.legal_entity_country_code = 'USA'
        row.legal_entity_country_name = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.legal_entity_country_name and row.legal_entity_country_code\
            and row.legal_entity_country_code.upper() in country_list:
        row.legal_entity_country_name = country_list[row.legal_entity_country_code]


def fix_fabs_ppop_country(row, country_list, state_by_code):
    """ Update ppop country code/name """
    # replace ppop country codes from US territories with USA, move them into the state slot
    if row.place_of_perform_country_c.upper() in country_code_map and row.place_of_perform_country_c.upper() != 'USA':
        row.place_of_perfor_state_code = country_code_map[row.place_of_perform_country_c.upper()]
        # only add the description if it's in our list
        if row.place_of_perfor_state_code in state_by_code:
            row.place_of_perform_state_nam = state_by_code[row.place_of_perfor_state_code]
        row.place_of_perform_country_c = 'USA'
        row.place_of_perform_country_n = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.place_of_perform_country_n and row.place_of_perform_country_c\
            and row.place_of_perform_country_c.upper() in country_list:
        row.place_of_perform_country_n = country_list[row.place_of_perform_country_c.upper()]


def fix_fabs_le_state(sess, row, state_by_code, state_by_name):
    """ Update legal entity state info """
    zip_data = None
    zip_check = False

    # derive state code from name
    if not row.legal_entity_state_code:
        # if we have a valid state name, just use that
        if row.legal_entity_state_name and row.legal_entity_state_name.upper() in state_by_name:
            row.legal_entity_state_code = state_by_name[row.legal_entity_state_name.upper()]
        # if we don't have a valid state name, we have to get more creative
        elif row.legal_entity_zip5:
            zip_code = row.legal_entity_zip5
            # if we have a 4-digit zip to go with the 5-digit, combine them
            if row.legal_entity_zip_last4:
                zip_code += row.legal_entity_zip_last4
            zip_data = get_zip_data(sess, zip_code)
            zip_check = True
            # if we have zip data, set the state code
            if zip_data:
                row.legal_entity_state_code = zip_data.state_abbreviation

    # derive state name if we have the code and no name
    if not row.legal_entity_state_name and row.legal_entity_state_code\
            and row.legal_entity_state_code.upper() in state_by_code:
        row.legal_entity_state_name = state_by_code[row.legal_entity_state_code.upper()]

    return zip_data, zip_check


def fix_fabs_ppop_state(sess, row, state_by_code, state_code_by_fips, state_by_name):
    """ Update ppop state info """
    zip_data = None
    zip_check = False

    # derive state code (none of them have it, but we should still check in case this gets run after the new
    # derivations go in)
    if not row.place_of_perfor_state_code:
        if us_ppop(row.place_of_performance_code, state_by_code, state_code_by_fips):
            state_code = row.place_of_performance_code[:2].upper()
            if state_code in state_by_code:
                row.place_of_perfor_state_code = state_code
            else:
                row.place_of_perfor_state_code = state_code_by_fips[state_code]
        elif row.place_of_perform_state_nam and row.place_of_perform_state_nam.upper() in state_by_name:
                row.place_of_perfor_state_code = state_by_name[row.place_of_perform_state_nam.upper()]
        else:
            zip_data = get_zip_data(sess, row.place_of_performance_zip4a)
            zip_check = True
            # if we got any data from this, get the state code based on it
            if zip_data:
                row.place_of_perfor_state_code = zip_data.state_abbreviation

    if not row.place_of_perform_state_nam and row.place_of_perfor_state_code\
            and row.place_of_perfor_state_code.upper() in state_by_code:
        row.place_of_perform_state_nam = state_by_code[row.place_of_perfor_state_code.upper()]

    return zip_data, zip_check


def fix_fabs_le_county(sess, row, zip_data, zip_check, county_by_code):
    """ Update legal entity county info """
    state = row.legal_entity_state_code
    # fill in legal entity county code where needed/possible
    if not row.legal_entity_county_code:
        if row.record_type == 1 and row.place_of_performance_code and \
                re.match('^([A-Z]{2}|\d{2})\*\*\d{3}$', row.place_of_performance_code.upper()):
            row.legal_entity_county_code = row.place_of_performance_code[-3:]
        elif row.legal_entity_zip5:
            # only grab new zip data if we don't have any to begin with for whatever reason and haven't tried to get it
            if not zip_data and not zip_check:
                zip_code = row.legal_entity_zip5
                # if we have a 4-digit zip to go with the 5-digit, combine them
                if row.legal_entity_zip_last4:
                    zip_code += row.legal_entity_zip_last4
                zip_data = get_zip_data(sess, zip_code)
            if zip_data:
                row.legal_entity_county_code = zip_data.county_number

    # fill in legal entity county name where needed/possible
    if not row.legal_entity_county_name and row.legal_entity_county_code and state:
        if state.upper() in county_by_code and row.legal_entity_county_code in county_by_code[state.upper()]:
            row.legal_entity_county_name = county_by_code[state.upper()][row.legal_entity_county_code]


def fix_fabs_ppop_county(sess, row, zip_data, zip_check, county_by_code):
    """ Update ppop county info """
    state = row.place_of_perfor_state_code
    # fill the place of performance county code where needed/possible
    if not row.place_of_perform_county_co:
        # we only need to check place of performance code if it exists and if we have a valid state
        if row.place_of_performance_code and state:
            ppop_code = row.place_of_performance_code.upper()
            # if county style, get county code
            if re.match('^([A-Z]{2}|\d{2})\*\*\d{3}$', ppop_code):
                row.place_of_perform_county_co = ppop_code[-3:]
            # if city style, check city code table
            elif re.match('^([A-Z]{2}|\d{2})\d{5}$', ppop_code):
                city_info = sess.query(CityCode).filter_by(city_code=ppop_code[-5:], state_code=state).first()
                # only set it if we got one
                if city_info:
                    row.place_of_perform_county_co = city_info.county_number
        # check if we managed to fill it in and if we have a zip4
        if not row.place_of_perform_county_co and row.place_of_performance_zip4a:
            # only look for zip data if we don't have any already and haven't tried to get it
            if not zip_data and not zip_check:
                zip_code = row.place_of_performance_zip4a
                zip_data = get_zip_data(sess, zip_code)
            if zip_data:
                row.place_of_perform_county_co = zip_data.county_number

    # fill in place of performance county name where needed/possible
    if not row.place_of_perform_county_na and row.place_of_perform_county_co and state:
        if state.upper() in county_by_code and row.place_of_perform_county_co in county_by_code[state.upper()]:
            row.place_of_perform_county_na = county_by_code[state.upper()][row.place_of_perform_county_co]


def process_fabs_derivations(sess, data, country_list, state_by_code, state_code_by_fips, state_by_name,
                             county_by_code):
    """ Process derivations for FABS location data """
    for row in data:
        # Don't update the updated_at timestamp
        row.ignore_updated_at = True

        # only run country adjustments if we have a country code
        if row.legal_entity_country_code:
            fix_fabs_le_country(row, country_list, state_by_code)

        # only run country adjustments if we have a country code
        if row.place_of_perform_country_c:
            fix_fabs_ppop_country(row, country_list, state_by_code)

        # clean up historical legal entity congressional districts that were stored as floats
        if row.legal_entity_congressional and '.' in row.legal_entity_congressional:
            row.legal_entity_congressional = clean_stored_float(row.legal_entity_congressional, 2)

        # clean up historical ppop congressional districts that were stored as floats
        if row.place_of_performance_congr and '.' in row.place_of_performance_congr:
            row.place_of_performance_congr = clean_stored_float(row.place_of_performance_congr, 2)

        # only do all of the following ppop derivations/checks if the country code is USA
        if row.place_of_perform_country_c and row.place_of_perform_country_c.upper() == 'USA':
            # fix state data
            ppop_zip_data, ppop_zip_check = fix_fabs_ppop_state(sess, row, state_by_code, state_code_by_fips,
                                                                state_by_name)

            # fix ppop county data
            fix_fabs_ppop_county(sess, row, ppop_zip_data, ppop_zip_check, county_by_code)

            # if we have a zip code from the US, split the 9-digit into a 5 and 4 digit when possible
            # we only need to do this for ppop for FABS because legal entity comes in split
            if row.place_of_performance_zip4a:
                ppop_zip5, ppop_zip4 = split_zip(row.place_of_performance_zip4a)
                row.place_of_performance_zip5 = ppop_zip5
                row.place_of_perform_zip_last4 = ppop_zip4

        # only do all of the following legal entity derivations/checks if the country code is USA
        if row.legal_entity_country_code and row.legal_entity_country_code.upper() == 'USA':
            # fix legal entity state data
            le_zip_data, le_zip_check = fix_fabs_le_state(sess, row, state_by_code, state_by_name)

            # fix legal entity county data
            fix_fabs_le_county(sess, row, le_zip_data, le_zip_check, county_by_code)


def update_historical_fabs(sess, country_list, state_by_code, state_code_by_fips, state_by_name, county_by_code, start,
                           end):
    """ Update historical FABS location data with new columns and missing data where possible """
    model = PublishedAwardFinancialAssistance
    start_slice = 0
    logger.info("Starting fabs update for: %s to %s", start, end)
    while True:
        query_result = sess.query(model).\
            filter(model.is_active.is_(True)).\
            filter(cast(model.action_date, Date) >= start).\
            filter(cast(model.action_date, Date) <= end).\
            slice(start_slice, start_slice + QUERY_SIZE).all()

        # break the loop
        if len(query_result) == 0:
            break

        logger.info("Updating records: %s to %s", str(start_slice), str(start_slice + QUERY_SIZE))
        # process the derivations for historical data
        process_fabs_derivations(sess, query_result, country_list, state_by_code, state_code_by_fips, state_by_name,
                                 county_by_code)
        start_slice += QUERY_SIZE
    sess.commit()
    logger.info("Finished fabs update for: %s to %s", start, end)


def main():
    parser = argparse.ArgumentParser(description='Update county information for historical FABS and FPDS data')
    parser.add_argument('-t', '--type', help='Which data type, argument must be fpds or fabs', nargs=1, type=str,
                        required=True)
    parser.add_argument('-s', '--start', help='Start date (inclusive), must be in the format YYYY/MM/DD or YYYY-MM-DD',
                        nargs=1, type=str, required=True)
    parser.add_argument('-e', '--end', help='End date (inclusive), must be in the format YYYY/MM/DD or YYYY-MM-DD',
                        nargs=1, type=str, required=True)
    args = parser.parse_args()

    sess = GlobalDB.db().session

    data_type = args.type[0]

    # get and create list of country code -> name mappings
    countries = sess.query(CountryCode).all()
    country_list = {}

    for country in countries:
        country_list[country.country_code] = country.country_name

    # get and create list of state code -> state name mappings. Prime the county lists with state codes
    county_by_name = {}
    county_by_code = {}
    state_by_code = {}
    state_by_name = {}
    state_code_by_fips = {}
    states = sess.query(States).all()

    for state in states:
        county_by_name[state.state_code] = {}
        county_by_code[state.state_code] = {}

        # we want to capitalize it if it's FPDS because that's how we store it
        state_name = state.state_name
        state_code = state.state_code
        if data_type == 'fpds':
            state_name = state_name.upper()
        state_by_code[state_code] = state_name
        state_code_by_fips[state.fips_code] = state_code
        state_by_name[state_name.upper()] = state_code

    # Fill the county lists with data (code -> name mappings and name -> code mappings)
    county_codes = sess.query(CountyCode.county_number, CountyCode.state_code, CountyCode.county_name).all()

    for county_code in county_codes:
        state_code = county_code.state_code
        county_num = county_code.county_number
        county_name = county_code.county_name.strip()

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
        update_historical_fabs(sess, country_list, state_by_code, state_code_by_fips, state_by_name, county_by_code,
                               args.start[0], args.end[0])
    else:
        logger.error("Type must be fpds or fabs.")


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
