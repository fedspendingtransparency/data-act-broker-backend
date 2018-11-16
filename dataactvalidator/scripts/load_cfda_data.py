import os
import logging

import pandas as pd
import boto3

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CFDAProgram
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe, format_date

logger = logging.getLogger(__name__)


def load_cfda_program(base_path):
    """ Load cfda program.

        Args:
            base_path: directory that contains the cfda values files.
    """
    if CONFIG_BROKER["use_aws"]:
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        filename = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                   'Key': "cfda_program.csv"}, ExpiresIn=600)
    else:
        filename = os.path.join(base_path, "cfda_program.csv")

    logger.info('Loading CFDA program file: ' + "cfda_program.csv")
    """Load country code lookup table."""
    model = CFDAProgram

    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str, encoding='latin1')

        data = clean_data(
            data,
            model,
            {"program_title": "program_title",
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
             "archived_date": "archived_date"},
            {}
        )
        data["published_date"] = format_date(data["published_date"])
        data["archived_date"] = format_date(data["archived_date"])

        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


if __name__ == '__main__':
    configure_logging()
