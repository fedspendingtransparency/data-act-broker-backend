import argparse
import logging
import re

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import CountryCode, States, CountyCode, CityCode, Zips
from dataactcore.models.stagingModels import PublishedFABS, DetachedAwardProcurement

from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

QUERY_SIZE = 3000
COMMIT_SIZE = 25000
ZIP_SLICE = 1000000
country_code_map = {'USA': 'US', 'ASM': 'AS', 'GUM': 'GU', 'MNP': 'MP', 'PRI': 'PR', 'VIR': 'VI', 'FSM': 'FM',
                    'MHL': 'MH', 'PLW': 'PW', 'XBK': 'UM', 'XHO': 'UM', 'XJV': 'UM', 'XJA': 'UM', 'XKR': 'UM',
                    'XPL': 'UM', 'XMW': 'UM', 'XWK': 'UM'}
g_country_list = {}
g_state_by_code = {}
g_state_code_by_fips = {}
g_state_by_name = {}
g_zip_list = {}
g_county_by_city = {}
g_county_by_code = {}
g_county_by_name = {}


def date_to_string(date):
    return date.strftime("%Y/%m/%d")


def valid_zip(zip_code):
    if not re.match('^\d{5}(-?\d{4})?$', zip_code):
        return False
    return True


def get_zip_data(zip_code):
    """ Get the county code based on the zip passed """
    # if it isn't a valid zip format or someone passed None, just toss back nothing
    if not zip_code or not valid_zip(zip_code):
        return None

    zip_data = None
    # if the 5-digit zip is in our list at all, then check details
    if zip_code[:5] in g_zip_list:
        zip5 = zip_code[:5]
        # if it's a 9 digit zip, try with both halves first
        if len(zip_code) > 5:
            if zip_code[-4:] in g_zip_list[zip5]:
                zip_data = g_zip_list[zip5][zip_code[-4:]]
        # if we didn't go into the last if statement or returned nothing from our previous search, try just 5 digit
        if not zip_data:
            zip_data = g_zip_list[zip5]['default']

    return zip_data


def clean_stored_float(clean_string, fill_amount):
    """ Remove the . from the given string and fill it (with zeroes) to the length we want """
    clean_string = re.sub('\..+', '', clean_string).zfill(fill_amount)
    return clean_string


def us_ppop(ppop):
    """ Determines if the ppop is in a valid format to be in the US """
    # return false if it's null or not 7 digits long
    if not ppop or len(ppop) != 7:
        return False

    ppop = ppop.upper()
    if ppop[:2] in g_state_by_code or ppop[:2] in g_state_code_by_fips:
        return True

    return False


def split_zip(zip_code):
    """ split the zip code into 5 and 4 digit codes """
    if not valid_zip(zip_code):
        return None, None

    if len(zip_code) == 5:
        return zip_code[:5], None

    return zip_code[:5], zip_code[-4:]


def fix_fabs_le_country(row):
    """ Update legal entity country code/name """
    # replace legal entity country codes from US territories with USA, move them into the state slot
    if row.legal_entity_country_code.upper() in country_code_map and row.legal_entity_country_code.upper() != 'USA':
        row.legal_entity_state_code = country_code_map[row.legal_entity_country_code.upper()]
        # only add the description if it's in our list
        if row.legal_entity_state_code in g_state_by_code:
            row.legal_entity_state_name = g_state_by_code[row.legal_entity_state_code]
        row.legal_entity_country_code = 'USA'
        row.legal_entity_country_name = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.legal_entity_country_name and row.legal_entity_country_code\
            and row.legal_entity_country_code.upper() in g_country_list:
        row.legal_entity_country_name = g_country_list[row.legal_entity_country_code.upper()]


def fix_fabs_ppop_country(row):
    """ Update ppop country code/name """
    # replace ppop country codes from US territories with USA, move them into the state slot
    if row.place_of_perform_country_c.upper() in country_code_map and row.place_of_perform_country_c.upper() != 'USA':
        row.place_of_perfor_state_code = country_code_map[row.place_of_perform_country_c.upper()]
        # only add the description if it's in our list
        if row.place_of_perfor_state_code in g_state_by_code:
            row.place_of_perform_state_nam = g_state_by_code[row.place_of_perfor_state_code]
        row.place_of_perform_country_c = 'USA'
        row.place_of_perform_country_n = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.place_of_perform_country_n and row.place_of_perform_country_c\
            and row.place_of_perform_country_c.upper() in g_country_list:
        row.place_of_perform_country_n = g_country_list[row.place_of_perform_country_c.upper()]


def fix_fabs_le_state(row):
    """ Update legal entity state info """
    zip_data = None
    zip_check = False

    # derive state code from name
    if not row.legal_entity_state_code:
        # if we have a valid state name, just use that
        if row.legal_entity_state_name and row.legal_entity_state_name.upper() in g_state_by_name:
            row.legal_entity_state_code = g_state_by_name[row.legal_entity_state_name.upper()]
        # if we don't have a valid state name, we have to get more creative
        elif row.legal_entity_zip5:
            zip_code = row.legal_entity_zip5
            # if we have a 4-digit zip to go with the 5-digit, combine them
            if row.legal_entity_zip_last4:
                zip_code += row.legal_entity_zip_last4
            zip_data = get_zip_data(zip_code)
            zip_check = True
            # if we have zip data, set the state code
            if zip_data:
                row.legal_entity_state_code = zip_data['state_abbreviation']

    # derive state name if we have the code and no name
    if not row.legal_entity_state_name and row.legal_entity_state_code\
            and row.legal_entity_state_code.upper() in g_state_by_code:
        row.legal_entity_state_name = g_state_by_code[row.legal_entity_state_code.upper()]

    return zip_data, zip_check


def fix_fabs_ppop_state(row):
    """ Update ppop state info """
    zip_data = None
    zip_check = False

    # derive state code (none of them have it, but we should still check in case this gets run after the new
    # derivations go in)
    if not row.place_of_perfor_state_code:
        if us_ppop(row.place_of_performance_code):
            state_code = row.place_of_performance_code[:2].upper()
            if state_code in g_state_by_code:
                row.place_of_perfor_state_code = state_code
            else:
                row.place_of_perfor_state_code = g_state_code_by_fips[state_code]
        elif row.place_of_perform_state_nam and row.place_of_perform_state_nam.upper() in g_state_by_name:
            row.place_of_perfor_state_code = g_state_by_name[row.place_of_perform_state_nam.upper()]
        else:
            zip_data = get_zip_data(row.place_of_performance_zip4a)
            zip_check = True
            # if we got any data from this, get the state code based on it
            if zip_data:
                row.place_of_perfor_state_code = zip_data['state_abbreviation']

    if not row.place_of_perform_state_nam and row.place_of_perfor_state_code\
            and row.place_of_perfor_state_code.upper() in g_state_by_code:
        row.place_of_perform_state_nam = g_state_by_code[row.place_of_perfor_state_code.upper()]

    return zip_data, zip_check


def fix_fabs_le_county(row, zip_data, zip_check):
    """ Update legal entity county info """
    state_code = row.legal_entity_state_code
    if state_code:
        state_code = state_code.upper()

    # remove . from county code, only legal entity has any of these
    if row.legal_entity_county_code and '.' in row.legal_entity_county_code:
        row.legal_entity_county_code = clean_stored_float(row.legal_entity_county_code, 3)

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
                zip_data = get_zip_data(zip_code)
            if zip_data:
                row.legal_entity_county_code = zip_data['county_number']

    # fill in legal entity county name where needed/possible
    if not row.legal_entity_county_name and row.legal_entity_county_code and state_code:
        if state_code in g_county_by_code and row.legal_entity_county_code in g_county_by_code[state_code]:
            row.legal_entity_county_name = g_county_by_code[state_code][row.legal_entity_county_code]


def fix_fabs_ppop_county(row, zip_data, zip_check):
    """ Update ppop county info """
    state_code = row.place_of_perfor_state_code
    if state_code:
        state_code = state_code.upper()
    # fill the place of performance county code where needed/possible
    if not row.place_of_perform_county_co:
        # we only need to check place of performance code if it exists and if we have a valid state
        if row.place_of_performance_code and state_code:
            ppop_code = row.place_of_performance_code.upper()
            # if county style, get county code
            if re.match('^([A-Z]{2}|\d{2})\*\*\d{3}$', ppop_code):
                row.place_of_perform_county_co = ppop_code[-3:]
            # if city style, check city code table
            elif re.match('^([A-Z]{2}|\d{2})\d{5}$', ppop_code):
                # only set it if we have this data in the list
                if ppop_code in g_county_by_city:
                    row.place_of_perform_county_co = g_county_by_city[state_code + ppop_code[-5:]]
        # check if we managed to fill it in and if we have a zip4
        if not row.place_of_perform_county_co and row.place_of_performance_zip4a:
            # only look for zip data if we don't have any already and haven't tried to get it
            if not zip_data and not zip_check:
                zip_code = row.place_of_performance_zip4a
                zip_data = get_zip_data(zip_code)
            if zip_data:
                row.place_of_perform_county_co = zip_data['county_number']

    # fill in place of performance county name where needed/possible
    if not row.place_of_perform_county_na and row.place_of_perform_county_co and state_code:
        if state_code in g_county_by_code and row.place_of_perform_county_co in g_county_by_code[state_code]:
            row.place_of_perform_county_na = g_county_by_code[state_code][row.place_of_perform_county_co]


def process_fabs_derivations(data):
    """ Process derivations for FABS location data """
    for row in data:
        # Don't update the updated_at timestamp
        row.ignore_updated_at = True

        # only run country adjustments if we have a country code
        if row.legal_entity_country_code:
            fix_fabs_le_country(row)

        # only run country adjustments if we have a country code
        if row.place_of_perform_country_c:
            fix_fabs_ppop_country(row)

        # clean up historical legal entity congressional districts that were stored as floats
        if row.legal_entity_congressional and '.' in row.legal_entity_congressional:
            row.legal_entity_congressional = clean_stored_float(row.legal_entity_congressional, 2)

        # clean up historical ppop congressional districts that were stored as floats
        if row.place_of_performance_congr and '.' in row.place_of_performance_congr:
            row.place_of_performance_congr = clean_stored_float(row.place_of_performance_congr, 2)

        # only do all of the following ppop derivations/checks if the country code is USA
        if row.place_of_perform_country_c and row.place_of_perform_country_c.upper() == 'USA':
            # fix state data
            ppop_zip_data, ppop_zip_check = fix_fabs_ppop_state(row)

            # fix ppop county data
            fix_fabs_ppop_county(row, ppop_zip_data, ppop_zip_check)

            # if we have a zip code from the US, split the 9-digit into a 5 and 4 digit when possible
            # we only need to do this for ppop for FABS because legal entity comes in split
            if row.place_of_performance_zip4a:
                ppop_zip5, ppop_zip4 = split_zip(row.place_of_performance_zip4a)
                row.place_of_performance_zip5 = ppop_zip5
                row.place_of_perform_zip_last4 = ppop_zip4

        # only do all of the following legal entity derivations/checks if the country code is USA
        if row.legal_entity_country_code and row.legal_entity_country_code.upper() == 'USA':
            # fix legal entity state data
            le_zip_data, le_zip_check = fix_fabs_le_state(row)

            # fix legal entity county data
            fix_fabs_le_county(row, le_zip_data, le_zip_check)


def update_historical_fabs(sess, start, end):
    """ Update historical FABS location data with new columns and missing data where possible """
    start_slice = start
    found_records = 0
    logger.info("Starting fabs update for ids: %s to %s", start, end)
    while True:
        end_slice = start_slice + QUERY_SIZE if start_slice + QUERY_SIZE < end else end
        query_result = sess.query(PublishedFABS).\
            filter(PublishedFABS.is_active.is_(True)).\
            filter(PublishedFABS.published_fabs_id >= start_slice).\
            filter(PublishedFABS.published_fabs_id <= end_slice).all()
        found_records += len(query_result)

        logger.info("Updating records: %s to %s", str(start_slice), str(end_slice))
        # process the derivations for historical data
        process_fabs_derivations(query_result)
        if found_records >= COMMIT_SIZE:
            logger.info("Pushing %s records to the DB", str(found_records))
            found_records = 0
            sess.commit()

        # break the loop if we've hit the last records
        if end_slice == end:
            logger.info("Pushing remaining %s records to the DB", str(found_records))
            break

        start_slice = end_slice + 1

    sess.commit()
    logger.info("Finished fabs update for ids: %s to %s", start, end)


def fix_fpds_le_country(row):
    """ Update legal entity country code/name """
    # replace legal entity country codes from US territories with USA, move them into the state slot
    if row.legal_entity_country_code.upper() in country_code_map and row.legal_entity_country_code.upper() != 'USA':
        row.legal_entity_state_code = country_code_map[row.legal_entity_country_code.upper()]
        # only add the description if it's in our list
        if row.legal_entity_state_code in g_state_by_code:
            row.legal_entity_state_descrip = g_state_by_code[row.legal_entity_state_code]
        row.legal_entity_country_code = 'USA'
        row.legal_entity_country_name = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.legal_entity_country_name and row.legal_entity_country_code\
            and row.legal_entity_country_code.upper() in g_country_list:
        row.legal_entity_country_name = g_country_list[row.legal_entity_country_code.upper()]


def fix_fpds_ppop_country(row):
    """ Update ppop country code/name """
    # replace ppop country codes from US territories with USA, move them into the state slot
    if row.place_of_perform_country_c.upper() in country_code_map and row.place_of_perform_country_c.upper() != 'USA':
        row.place_of_performance_state = country_code_map[row.place_of_perform_country_c.upper()]
        # only add the description if it's in our list
        if row.place_of_performance_state in g_state_by_code:
            row.place_of_perfor_state_desc = g_state_by_code[row.place_of_performance_state]
        row.place_of_perform_country_c = 'USA'
        row.place_of_perf_country_desc = 'UNITED STATES'

    # grab the country name if we have access to it and it isn't already there
    if not row.place_of_perf_country_desc and row.place_of_perform_country_c\
            and row.place_of_perform_country_c.upper() in g_country_list:
        row.place_of_perf_country_desc = g_country_list[row.place_of_perform_country_c.upper()]


def fix_fpds_le_state(row):
    """ Update legal entity state info """
    # there are several instances where state code is 'nan' in le, which is simply poor storage, clear those out.
    if row.legal_entity_state_code == 'nan':
        row.legal_entity_state_code = None

    if not row.legal_entity_state_descrip and row.legal_entity_state_code\
            and row.legal_entity_state_code.upper() in g_state_by_code:
        row.legal_entity_state_descrip = g_state_by_code[row.legal_entity_state_code.upper()]


def fix_fpds_ppop_state(row):
    """ Update place of performance state info """
    # fill in the state name where possible
    if not row.place_of_perfor_state_desc and row.place_of_performance_state:
        state_code = row.place_of_performance_state.upper()
        # there are several cases in ppop where state codes are stored as FIPS codes
        if state_code in g_state_code_by_fips:
            state_code = g_state_code_by_fips[state_code]

        # if the state code (the one given or the one derived above from FIPS) is in the state list, get the name
        if state_code in g_state_by_code:
            row.place_of_perfor_state_desc = g_state_by_code[state_code]


def fix_fpds_le_cd(row):
    # if the CD is a mashup of CD and state code, get just the last 2 characters (which are the CD)
    if re.match('.+\d\d$', row.legal_entity_congressional):
        row.legal_entity_congressional = row.legal_entity_congressional[-2:]
    # if it's ZZ, just clear it out
    elif row.legal_entity_congressional == 'ZZ':
        row.legal_entity_congressional = None


def fix_fpds_ppop_cd(row):
    # if the CD is a mashup of CD and state code, get just the last 2 characters (which are the CD)
    if re.match('.+\d\d$', row.place_of_performance_congr):
        row.place_of_performance_congr = row.place_of_performance_congr[-2:]
    # if it's ZZ, just clear it out
    elif row.place_of_performance_congr == 'ZZ':
        row.place_of_performance_congr = None


def fix_fpds_le_county(row):
    """ These are both new columns, so as long as we have the data, we want to try to derive them """
    # we only want to do the below if we're missing either the county code or name, don't access the zip DB if we don't
    # have to
    if not row.legal_entity_county_code or not row.legal_entity_county_name:

        zip_data = get_zip_data(row.legal_entity_zip4)
        if zip_data:
            if not row.legal_entity_county_code:
                row.legal_entity_county_code = zip_data['county_number']

            # if we got the zip data and have a state code to work with, if it's valid then grab the county name
            if not row.legal_entity_county_name and row.legal_entity_state_code:
                state_code = row.legal_entity_state_code.upper()
                county_code = row.legal_entity_county_code

                if state_code in g_county_by_code and county_code in g_county_by_code[state_code]:
                    row.legal_entity_county_name = g_county_by_code[state_code][county_code]


def fix_fpds_ppop_county(row):
    """ Derive ppop county code and name (where possible/missing) """
    state_code = row.place_of_performance_state
    if state_code:
        state_code = state_code.upper()
    # if we have the county name and state code, derive the name based on those
    if not row.place_of_perform_county_co:
        if row.place_of_perform_county_na and state_code:
            county_name = row.place_of_perform_county_na.upper()

            if state_code in g_county_by_name and county_name in g_county_by_name[state_code]:
                row.place_of_perform_county_co = g_county_by_name[state_code][county_name]

        # if we still don't have a county code, try the zip
        if not row.place_of_perform_county_co and row.place_of_performance_zip4a:
            zip_data = get_zip_data(row.place_of_performance_zip4a)

            if zip_data:
                row.place_of_perform_county_co = zip_data['county_number']

    # if we don't have the county name but have the county code, derive the name
    if not row.place_of_perform_county_na and state_code in g_county_by_code\
            and row.place_of_perform_county_co in g_county_by_code[state_code]:
        row.place_of_perform_county_na = g_county_by_code[state_code][row.place_of_perform_county_co]


def process_fpds_derivations(data):
    """ Process derivations for FPDS location data """
    for row in data:
        # Don't update the updated_at timestamp
        row.ignore_updated_at = True

        # only run country adjustments if we have a country code
        if row.legal_entity_country_code:
            fix_fpds_le_country(row)

        # only run country adjustments if we have a country code
        if row.place_of_perform_country_c:
            fix_fpds_ppop_country(row)

        # only do all of the following ppop derivations/checks if the country code is USA
        if row.place_of_perform_country_c and row.place_of_perform_country_c.upper() == 'USA':
            # fix state data
            fix_fpds_ppop_state(row)

            # fix congressional district data (but only if we have the cd)
            if row.place_of_performance_congr:
                fix_fpds_ppop_cd(row)

            # fix county data
            fix_fpds_ppop_county(row)

            if row.place_of_performance_zip4a:
                ppop_zip5, ppop_zip4 = split_zip(row.place_of_performance_zip4a)
                row.place_of_performance_zip5 = ppop_zip5
                row.place_of_perform_zip_last4 = ppop_zip4

        # only do all of the following legal entity derivations/checks if the country code is USA
        if row.legal_entity_country_code and row.legal_entity_country_code.upper() == 'USA':
            # fix state data
            fix_fpds_le_state(row)

            # fix congressional district data (but only if we have the cd)
            if row.legal_entity_congressional:
                fix_fpds_le_cd(row)

            if row.legal_entity_zip4:
                # we only need to try to derive legal entity county code if we were given a zip to work with
                fix_fpds_le_county(row)

                le_zip5, le_zip4 = split_zip(row.legal_entity_zip4)
                row.legal_entity_zip5 = le_zip5
                row.legal_entity_zip_last4 = le_zip4


def update_historical_fpds(sess, start, end):
    """ Update historical FPDS location data with new columns and missing data where possible """
    model = DetachedAwardProcurement
    start_slice = start
    found_records = 0
    logger.info("Starting fpds update for ids: %s to %s", start, end)
    while True:
        end_slice = start_slice + QUERY_SIZE if start_slice + QUERY_SIZE < end else end
        query_result = sess.query(model). \
            filter(model.detached_award_procurement_id >= start_slice). \
            filter(model.detached_award_procurement_id <= end_slice).all()
        found_records += len(query_result)

        logger.info("Updating records: %s to %s", str(start_slice), str(end_slice))
        # process the derivations for historical data
        process_fpds_derivations(query_result)
        if found_records >= COMMIT_SIZE:
            logger.info("Pushing %s records to the DB", str(found_records))
            found_records = 0
            sess.commit()

        # break the loop if we've hit the last records
        if end_slice == end:
            logger.info("Pushing remaining %s records to the DB", str(found_records))
            break

        start_slice = end_slice + 1

    sess.commit()
    logger.info("Finished fpds update for ids: %s to %s", start, end)


def main():
    parser = argparse.ArgumentParser(description='Update county information for historical FABS and FPDS data')
    parser.add_argument('-t', '--type', help='Which data type, argument must be fpds or fabs', nargs=1, type=str,
                        required=True)
    parser.add_argument('-s', '--start', help='Start id, must be number', nargs=1, type=int, required=True)
    parser.add_argument('-e', '--end', help='End id, must be number', nargs=1, type=int, required=True)
    args = parser.parse_args()

    sess = GlobalDB.db().session

    data_type = args.type[0]

    global g_country_list
    global g_state_by_code
    global g_state_code_by_fips
    global g_state_by_name
    global g_zip_list
    global g_county_by_city
    global g_county_by_code
    global g_county_by_name

    logger.info("Starting location dictionary compilation")

    # get and create list of country code -> name mappings
    countries = sess.query(CountryCode).all()

    for country in countries:
        g_country_list[country.country_code] = country.country_name
    del countries

    # get and create list of state code -> state name mappings. Prime the county lists with state codes
    states = sess.query(States).all()

    for state in states:
        g_county_by_name[state.state_code] = {}
        g_county_by_code[state.state_code] = {}

        # we want to capitalize it if it's FPDS because that's how we store it
        state_name = state.state_name
        state_code = state.state_code
        if data_type == 'fpds':
            state_name = state_name.upper()
        g_state_by_code[state_code] = state_name
        g_state_code_by_fips[state.fips_code] = state_code
        g_state_by_name[state_name.upper()] = state_code
    del states

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
        g_county_by_code[state_code][county_num] = county_name

        # if the county name has only letters/spaces then we want it in our by-name lookup, the rest have the
        # potential to be different from the FPDS feed (and won't be used in FABS)
        if re.match('^[A-Z\s]+$', county_name):
            g_county_by_name[state_code][county_name] = county_num
    del county_codes

    # pull in all city codes
    city_codes = sess.query(CityCode.city_code, CityCode.state_code, CityCode.county_number).all()

    for city_code in city_codes:
        g_county_by_city[city_code.state_code + city_code.city_code] = city_code.county_number
    del city_codes

    # pull in all the zip codes
    start_slice = 0
    while True:
        end_slice = start_slice + ZIP_SLICE
        zip_codes = sess.query(Zips.zip5, Zips.zip_last4, Zips.state_abbreviation, Zips.county_number).\
            slice(start_slice, end_slice).all()

        for zip_data in zip_codes:
            if zip_data.zip5 not in g_zip_list:
                g_zip_list[zip_data.zip5] = {}
                g_zip_list[zip_data.zip5]['default'] = {"state_abbreviation": zip_data.state_abbreviation,
                                                        "county_number": zip_data.county_number}
            g_zip_list[zip_data.zip5][zip_data.zip_last4] = {"state_abbreviation": zip_data.state_abbreviation,
                                                             "county_number": zip_data.county_number}

        logger.info("Added %s rows to zip dict", str(end_slice))

        start_slice = end_slice

        # break the loop if we've hit the last records
        if len(zip_codes) < ZIP_SLICE:
            break
    del zip_codes

    if data_type == 'fpds':
        update_historical_fpds(sess, args.start[0], args.end[0])
    elif data_type == 'fabs':
        update_historical_fabs(sess, args.start[0], args.end[0])
    else:
        logger.error("Type must be fpds or fabs.")

    # delete all global variables just in case
    del g_country_list
    del g_state_by_code
    del g_state_code_by_fips
    del g_state_by_name
    del g_zip_list
    del g_county_by_city
    del g_county_by_code
    del g_county_by_name

    logger.info("Completed location derivations")


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
