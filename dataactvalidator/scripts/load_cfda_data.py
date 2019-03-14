import os
import sys
import logging
import requests
import pandas as pd
import boto3
import time
import math
from datetime import datetime

from sqlalchemy import *

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CFDAProgram
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe, format_date

logger = logging.getLogger(__name__)

S3_CFDA_FILE = 'https://files.usaspending.gov/reference_data/cfda.csv'

DATA_CLEANING_MAP  = {
                 "program_title": "program_title",
                 "program_number": "program_number",
                 "popular_name_(020)": "popular_name",
                 "federal_agency_(030)": "federal_agency",
                 "authorization_(040)": "authorization",
                 "objectives_(050)": "objectives",
                 "types_of_assistance_(060)": "types_of_assistance",
                 "uses_and_use_restrictions_(070)": "uses_and_use_restrictions",
                 "applicant_eligibility_(081)": "applicant_eligibility",
                 "beneficiary_eligibility_(082)": "beneficiary_eligibility",
                 "credentials/documentation_(083)": "credentials_documentation",
                 "preapplication_coordination_(091)": "preapplication_coordination",
                 "application_procedures_(092)": "application_procedures",
                 "award_procedure_(093)": "award_procedure",
                 "deadlines_(094)": "deadlines",
                 "range_of_approval/disapproval_time_(095)": "range_of_approval_disapproval_time",
                 "appeals_(096)": "appeals",
                 "renewals_(097)": "renewals",
                 "formula_and_matching_requirements_(101)": "formula_and_matching_requirements",
                 "length_and_time_phasing_of_assistance_(102)": "length_and_time_phasing_of_assistance",
                 "reports_(111)": "reports",
                 "audits_(112)": "audits",
                 "records_(113)": "records",
                 "account_identification_(121)": "account_identification",
                 "obligations_(122)": "obligations",
                 "range_and_average_of_financial_assistance_(123)": "range_and_average_of_financial_assistance",
                 "program_accomplishments_(130)": "program_accomplishments",
                 "regulations__guidelines__and_literature_(140)": "regulations_guidelines_and_literature",
                 "regional_or__local_office_(151)": "regional_or_local_office",
                 "headquarters_office_(152)": "headquarters_office",
                 "website_address_(153)": "website_address",
                 "related_programs_(160)": "related_programs",
                 "examples_of_funded_projects_(170)": "examples_of_funded_projects",
                 "criteria_for_selecting_proposals_(180)": "criteria_for_selecting_proposals",
                 "url": "url",
                 "recovery": "recovery",
                 "omb_agency_code": "omb_agency_code",
                 "omb_bureau_code": "omb_bureau_code",
                 "published_date": "published_date",
                 "archived_date": "archived_date"
                 }

def cloneTable(name, table, metadata):
    cols = [c.copy() for c in table.columns]
    constraints = [c.copy() for c in table.constraints]
    return Table(name, metadata, *(cols + constraints))


def load_cfda_program(base_path, load_local=False, local_file_name="cfda_program.csv"):
    """ Load cfda program.

        Args:
            base_path: directory that contains the cfda values files.
    """ 
    if not load_local:
        logger.info("Fetching CFDA file from {}".format(S3_CFDA_FILE))
        tmp_name = str(time.time()).replace(".", "")+"_cfda_program.csv"
        filename = os.path.join(base_path, tmp_name)
        r = requests.get(S3_CFDA_FILE, allow_redirects=True)
        open(filename, 'wb').write(r.content)
    else:
        filename = os.path.join(base_path, local_file_name)

    logger.info('Loading CFDA program file: ' + filename)
    """Load country code lookup table."""
    model = CFDAProgram

    # TODO: Find out if we have a function like this somewhere already and/or move this to a util file.
    def round_up(n, decimals=0):
        multiplier = 10 ** decimals
        return math.floor(n*multiplier + 0.5) / multiplier

    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session


        # Witness the insanity...

        now = datetime.utcnow()       
        import_data = pd.read_csv(filename, dtype=str, encoding='latin1', na_filter=False)
        import_data = clean_data(
            import_data,
            model,
            DATA_CLEANING_MAP,
            {}
        )
        import_data["published_date"] = format_date(import_data["published_date"])
        import_data["archived_date"] = format_date(import_data["archived_date"])
        import_dataframe = import_data.copy(deep=True)
        #To do the comparison, first we need to mock the pk column that postgres creates. We'll set it unversally to 1
        import_dataframe = import_dataframe.assign(cfda_program_id = 1,created_at=now, updated_at=now)

        table_name = model.__table__.name
        current_data = pd.read_sql_table(table_name, sess.connection(), coerce_float=False)
        # Now we need to overwrite the db's audit dates in the created dataframe, and ALSO set all the  pks to 1, so they match
        current_data = current_data.assign(cfda_program_id=1,created_at=now, updated_at=now)
        #pandas comparison requires everything to be in the same order
        current_data.sort_values('program_number', inplace=True)
        import_dataframe.sort_values('program_number',inplace=True)
        
        #columns too
        cols = import_dataframe.columns.tolist()
        cols.sort()
        import_dataframe = import_dataframe[cols]

        cols = current_data.columns.tolist()
        cols.sort()
        current_data = current_data[cols]
        
        # need to reset the indexes now that we've done all this sorting, so that THEY match
        import_dataframe.reset_index(drop=True, inplace=True)
        current_data.reset_index(drop=True, inplace=True)
        #My favorite part: When pandas pulls the data out of postgres, the program_number column is a Decimal. However, in adding it to
        # the dataframe, this column loses precision, for reasons I could not uncover and cannot fathom. So for example, a program number of
        # 10.001 imports into the dataframe as 10.000999999999999. It also needs to be cast to a string, and padded with the right number of zero, as needed.
        current_data['program_number'] = current_data['program_number'].apply(lambda x: str(round_up(x, 3)).ljust(6, '0'))
        #Finally, you can exceute this and get True back if the data truly has not changed from the last time the CSV was loaded.
        new_data = not import_dataframe.equals(current_data)
        if new_data:
            #insert to db
            sess.query(model).delete()
            num = insert_dataframe(import_data, table_name, sess.connection())
            sess.commit()
            
        
    if new_data:
        logger.info('{} records inserted to {}'.format(num, table_name))
    else:
        logger.info("Skipped cfda load, no new data.")
    if not load_local:
        os.remove(filename)


if __name__ == '__main__':
    configure_logging()
