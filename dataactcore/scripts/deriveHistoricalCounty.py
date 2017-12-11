import argparse
import logging
import re

from sqlalchemy import func

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CountyCode, Zips

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


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

    county_codes = sess.query(CountyCode.county_number, CountyCode.state_code,
                              func.upper(CountyCode.county_name).label('county_name')).all()
    county_by_name = {}
    county_by_code = {}

    for county_code in county_codes:
        # insert state codes to each list if they aren't already in there
        if county_code.state_code not in county_by_name:
            county_by_name[county_code.state_code] = {}
        if county_code.state_code not in county_by_code:
            county_by_code[county_code.state_code] = {}

        # we don't want any "(CA)" endings, so strip those (also strip all extra whitespace)
        county_name = county_code.county_name.replace(' (CA)', '').strip()

        # we want all the counties in our by-code lookup because we'd be using this table anyway for derivations
        county_by_code[county_code.state_code][county_code.county_number] = county_name

        # if the county name has only letters/spaces then we want it in our by-name lookup, the rest have the potential
        # to be different from the FPDS feed
        if re.match('^[A-Z\s]+$', county_code.county_name):
            county_by_name[county_code.state_code][county_name] = county_code.county_number

    data_type = args.type[0]

    if data_type == 'fpds':
        print("fpds derivations")
    elif data_type == 'fabs':
        print("fabs derivations")
    else:
        logger.error("Type must be fpds or fabs.")


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()