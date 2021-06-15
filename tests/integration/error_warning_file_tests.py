import os
import csv
import logging
import itertools
import pandas as pd
import psutil as ps
from _pytest.monkeypatch import MonkeyPatch

from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_SERVICES
from dataactcore.models.domainModels import concat_tas_dict
from dataactcore.models.lookups import (FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT, RULE_SEVERITY_DICT)
from dataactcore.models.jobModels import Submission, Job, FileType
from dataactcore.models.userModel import User
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.stagingModels import (
    Appropriation, ObjectClassProgramActivity, AwardFinancial, FlexField, TotalObligations)
from dataactvalidator.health_check import create_app
import dataactvalidator.validation_handlers.validationManager
from dataactvalidator.validation_handlers.validationManager import (
    ValidationManager, FileColumn, CsvReader, parse_fields
)
import dataactvalidator.validation_handlers.validator
from dataactbroker.handlers.fileHandler import report_file_name

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from tests.integration.baseTestValidator import BaseTestValidator
from tests.integration.integration_test_helper import insert_submission, insert_job

FILES_DIR = os.path.join('tests', 'integration', 'data')

# Valid Files
APPROP_FILE = os.path.join(FILES_DIR, 'appropValid.csv')
AFINANCIAL_FILE = os.path.join(FILES_DIR, 'awardFinancialValid.csv')
CROSS_FILE_A = os.path.join(FILES_DIR, 'cross_file_A.csv')
CROSS_FILE_B = os.path.join(FILES_DIR, 'cross_file_B.csv')

# Invalid Files
HEADER_ERROR = os.path.join(FILES_DIR, 'appropHeaderError.csv')
READ_ERROR = os.path.join(FILES_DIR, 'appropReadError.csv')
LENGTH_ERROR = os.path.join(FILES_DIR, 'appropLengthError.csv')
TYPE_ERROR = os.path.join(FILES_DIR, 'appropTypeError.csv')
REQUIRED_ERROR = os.path.join(FILES_DIR, 'appropRequiredError.csv')
RULE_FAILED_WARNING = os.path.join(FILES_DIR, 'appropInvalidWarning.csv')
RULE_FAILED_ERROR = os.path.join(FILES_DIR, 'appropInvalidError.csv')
INVALID_CROSS_A = os.path.join(FILES_DIR, 'invalid_cross_file_A.csv')
INVALID_CROSS_B = os.path.join(FILES_DIR, 'invalid_cross_file_B.csv')
BLANK_C = os.path.join(FILES_DIR, 'awardFinancialBlank.csv')


class ErrorWarningTests(BaseTestValidator):
    """ Overall integration tests for error/warning reports.

        For each file type (single-file, cross-file, errors, warnings), test if each has
        - the correct structure
        - each column's content is correct after testing each possible type of error:
            - formatting
            - length
            - types
            - required/optional
            - SQL validation

        Attributes:
            session: the database session connection
            validator: validator instance to be used for the tests
            submission_id: the id of the submission foundation
            submission: the submission foundation to be used for all the tests
            val_job: the validation job to be used for all the tests
    """
    CHUNK_SIZES = [4]
    PARALLEL_OPTIONS = [True, False]
    BATCH_SQL_OPTIONS = [True, False]
    CONFIGS = list(itertools.product(CHUNK_SIZES, PARALLEL_OPTIONS, BATCH_SQL_OPTIONS))

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(ErrorWarningTests, cls).setUpClass()

        logging.getLogger('dataactcore').setLevel(logging.ERROR)
        logging.getLogger('dataactvalidator').setLevel(logging.ERROR)

        with create_app().app_context():
            cls.monkeypatch = MonkeyPatch()

            # get the submission test users
            sess = GlobalDB.db().session
            cls.session = sess

            # set up default e-mails for tests
            admin_user = sess.query(User).filter(User.email == cls.test_users['admin_user']).one()

            cls.validator = ValidationManager(directory=CONFIG_SERVICES['error_report_path'])

            # Just have one valid submission and then keep on reloading files
            cls.submission_id = insert_submission(sess, admin_user.user_id, cgac_code='SYS', start_date='01/2001',
                                                  end_date='03/2001', is_quarter=True)
            cls.submission = sess.query(Submission).filter_by(submission_id=cls.submission_id).one()
            cls.val_job = insert_job(cls.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['ready'],
                                     JOB_TYPE_DICT['csv_record_validation'], cls.submission_id,
                                     filename=JOB_TYPE_DICT['csv_record_validation'])
            cls.original_reports = set(os.listdir(CONFIG_SERVICES['error_report_path']))

            # adding TAS to ensure valid file is valid
            tas1 = TASFactory(account_num=1, allocation_transfer_agency='019', agency_identifier='072',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='0306', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas2 = TASFactory(account_num=2, allocation_transfer_agency=None, agency_identifier='019',
                              beginning_period_of_availa='2016', ending_period_of_availabil='2016',
                              availability_type_code=None, main_account_code='0113', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas3 = TASFactory(account_num=3, allocation_transfer_agency=None, agency_identifier='028',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='0406', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas4 = TASFactory(account_num=4, allocation_transfer_agency=None, agency_identifier='028',
                              beginning_period_of_availa='2010', ending_period_of_availabil='2011',
                              availability_type_code=None, main_account_code='0406', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas5 = TASFactory(account_num=5, allocation_transfer_agency='069', agency_identifier='013',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='2050', sub_account_code='005',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas6 = TASFactory(account_num=6, allocation_transfer_agency='028', agency_identifier='028',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='8007', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas7 = TASFactory(account_num=7, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas8 = TASFactory(account_num=8, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa='2010', ending_period_of_availabil='2011',
                              availability_type_code=None, main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas9 = TASFactory(account_num=9, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa='2014', ending_period_of_availabil='2015',
                              availability_type_code=None, main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000', financial_indicator2='F')
            tas10 = TASFactory(account_num=10, allocation_transfer_agency=None, agency_identifier='049',
                               beginning_period_of_availa='2015', ending_period_of_availabil='2016',
                               availability_type_code=None, main_account_code='0100', sub_account_code='000',
                               internal_start_date='01-01-2000')
            sess.add_all([tas1, tas2, tas3, tas4, tas5, tas6, tas7, tas8, tas9, tas10])

            # adding GTAS to ensure valid file is valid
            gtas1 = SF133Factory(tas=concat_tas_dict(tas1.component_dict()), allocation_transfer_agency='019',
                                 agency_identifier='072', beginning_period_of_availa=None, line=1009,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0306', sub_account_code='000', period=6, fiscal_year=2001)
            gtas2 = SF133Factory(tas=concat_tas_dict(tas2.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='019', beginning_period_of_availa='2016', line=1009,
                                 ending_period_of_availabil='2016', availability_type_code=None,
                                 main_account_code='0113', sub_account_code='000', period=6, fiscal_year=2001)
            gtas3 = SF133Factory(tas=concat_tas_dict(tas3.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='028', beginning_period_of_availa=None, line=1009,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0406', sub_account_code='000', period=6, fiscal_year=2001)
            gtas4 = SF133Factory(tas=concat_tas_dict(tas4.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='028', beginning_period_of_availa='2010', line=1009,
                                 ending_period_of_availabil='2011', availability_type_code=None,
                                 main_account_code='0406', sub_account_code='000', period=6, fiscal_year=2001)
            gtas5 = SF133Factory(tas=concat_tas_dict(tas5.component_dict()), allocation_transfer_agency='069',
                                 agency_identifier='013', beginning_period_of_availa=None, line=1009,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='2050', sub_account_code='005', period=6, fiscal_year=2001)
            gtas6 = SF133Factory(tas=concat_tas_dict(tas6.component_dict()), allocation_transfer_agency='028',
                                 agency_identifier='028', beginning_period_of_availa=None, line=1009,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='8007', sub_account_code='000', period=6, fiscal_year=2001)
            gtas7 = SF133Factory(tas=concat_tas_dict(tas7.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa=None, line=1009,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas8 = SF133Factory(tas=concat_tas_dict(tas8.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa='2010', line=1009,
                                 ending_period_of_availabil='2011', availability_type_code=None,
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas9 = SF133Factory(tas=concat_tas_dict(tas9.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa='2014', line=1009,
                                 ending_period_of_availabil='2015', availability_type_code=None,
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas10 = SF133Factory(tas=concat_tas_dict(tas10.component_dict()), allocation_transfer_agency=None,
                                  agency_identifier='049', beginning_period_of_availa='2015', line=1009,
                                  ending_period_of_availabil='2016', availability_type_code=None,
                                  main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            sess.add_all([gtas1, gtas2, gtas3, gtas4, gtas5, gtas6, gtas7, gtas8, gtas9, gtas10])
            sess.commit()

    def setUp(self):
        """Test set-up."""
        super(ErrorWarningTests, self).setUp()

    def get_report_path(self, file_type, warning=False, cross_type=None):
        filename = report_file_name(self.submission_id, warning, file_type, cross_type)
        return os.path.join(CONFIG_SERVICES['error_report_path'], filename)

    def setup_csv_record_validation(self, file, file_type):
        self.session.query(Job).delete(synchronize_session='fetch')
        self.val_job = insert_job(self.session, FILE_TYPE_DICT[file_type], JOB_STATUS_DICT['ready'],
                                  JOB_TYPE_DICT['csv_record_validation'], self.submission_id,
                                  filename=file)

    def setup_validation(self):
        self.session.query(Job).delete(synchronize_session='fetch')
        self.val_job = insert_job(self.session, None, JOB_STATUS_DICT['ready'],
                                  JOB_TYPE_DICT['validation'], self.submission_id,
                                  filename=None)

    def get_report_content(self, report_path, cross_file=False):
        report_content = []
        report_headers = None
        with open(report_path, 'r') as report_csv:
            reader = csv.DictReader(report_csv)
            for row in reader:
                report_content.append(row)
            report_headers = reader.fieldnames
        row_number_col = 'Row Number' if not cross_file else 'Source Row Number'
        if row_number_col in report_headers:
            report_content = list(sorted(report_content, key=lambda x: int(x[row_number_col] or 0)))
        return report_headers, report_content

    def generate_file_report(self, file, file_type, warning=False, ignore_error=False):
        self.setup_csv_record_validation(file, file_type)
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except Exception:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(file_type, warning=warning)
        report_content = self.get_report_content(report_path, cross_file=False)
        return report_content

    def generate_cross_file_report(self, cross_files, warning=False, ignore_error=False):
        cross_types = []
        for cross_file in cross_files:
            cross_types.append(cross_file[1])
            self.generate_file_report(cross_file[0], cross_file[1], warning=warning, ignore_error=ignore_error)

        self.setup_validation()
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except Exception:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(cross_types[0], cross_type=cross_types[1], warning=warning)
        report_content = self.get_report_content(report_path, cross_file=True)
        return report_content

    def cleanup(self):
        new_reports = set(os.listdir(CONFIG_SERVICES['error_report_path'])) - self.original_reports
        for new_report in new_reports:
            os.remove(os.path.join(CONFIG_SERVICES['error_report_path'], new_report))
        self.session.query(Appropriation).delete(synchronize_session='fetch')
        self.session.query(ObjectClassProgramActivity).delete(synchronize_session='fetch')
        self.session.query(AwardFinancial).delete(synchronize_session='fetch')
        self.session.query(ErrorMetadata).delete(synchronize_session='fetch')
        self.session.query(FlexField).delete(synchronize_session='fetch')
        self.session.commit()

    def test_single_file_warnings(self):
        for chunk_size, parallel, batch_sql in self.CONFIGS:
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'CHUNK_SIZE', chunk_size)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'PARALLEL', parallel)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'BATCH_SQL_VAL_RESULTS',
                                     batch_sql)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validator, 'SQL_VALIDATION_BATCH_SIZE',
                                     chunk_size)
            self.single_file_warnings()

    def single_file_warnings(self):
        self.cleanup()
        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE, 'appropriations', warning=True)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['warning']).count()
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 10
        assert error_count == 0
        assert report_headers == self.validator.report_headers
        assert len(report_content) == 0
        self.cleanup()

        # Blank File
        report_headers, report_content = self.generate_file_report(BLANK_C, 'award_financial', warning=True)
        awfin_count = self.session.query(AwardFinancial).filter_by(submission_id=self.submission_id).count()
        assert awfin_count == 0
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 0
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['warning']).count()
        assert self.validator.job.number_of_rows == 1
        assert self.validator.job.number_of_rows_valid == 2
        assert error_count == 1
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': '',
                'Field Name': 'Blank File',
                'Rule Message': 'File does not contain data. For files A and B, this must be addressed prior to'
                                ' publication/certification. Blank file C does not prevent publication/certification.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '',
                'Rule Label': 'DABSBLANK'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # SQL Validation
        report_headers, report_content = self.generate_file_report(RULE_FAILED_WARNING, 'appropriations', warning=True)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['warning']).count()
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 10
        assert error_count == 1
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 028-2010/2011-0406-000',
                'Field Name': 'budgetauthorityunobligatedbalancebroughtforward_fyb',
                'Rule Message': 'All the elements that have FYB in file A are expected in the first submission'
                                ' for a fiscal year',
                'Value Provided': 'budgetauthorityunobligatedbalancebroughtforward_fyb: ',
                'Expected Value': 'If the reporting period is Quarter 1, a non-null amount should be submitted for the'
                                  ' following elements: BudgetAuthorityUnobligatedBalanceBroughtForward_FYB',
                'Difference': '',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '5',
                'Rule Label': 'A16.1'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

    def test_single_file_errors(self):
        for chunk_size, parallel, batch_sql in self.CONFIGS:
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'CHUNK_SIZE', chunk_size)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'PARALLEL', parallel)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'BATCH_SQL_VAL_RESULTS',
                                     batch_sql)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validator, 'SQL_VALIDATION_BATCH_SIZE',
                                     chunk_size)
            self.single_file_errors()

    def single_file_errors(self):
        self.cleanup()

        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 10
        assert error_count == 0
        assert report_headers == self.validator.report_headers
        assert len(report_content) == 0
        self.cleanup()

        # Header Error
        report_headers, report_content = self.generate_file_report(HEADER_ERROR, 'appropriations', warning=False,
                                                                   ignore_error=True)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 0
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 0
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert self.validator.job.number_of_rows is None
        assert self.validator.job.number_of_rows_valid == 0
        # Header errors do not get saved to the database
        assert error_count == 0
        assert report_headers == ['Error type', 'Header name']
        expected_values = [
            {
                'Error type': 'Duplicated header',
                'Header name': 'AllocationTransferAgencyIdentifier'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'AdjustmentsToUnobligatedBalanceBroughtForward_CPE'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'AgencyIdentifier'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'BudgetAuthorityUnobligatedBalanceBroughtForward_FYB'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'DeobligationsRecoveriesRefundsByTAS_CPE'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'GrossOutlayAmountByTAS_CPE'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'ObligationsIncurredTotalByTAS_CPE'
            },
            {
                'Error type': 'Missing header',
                'Header name': 'StatusOfBudgetaryResourcesTotal_CPE'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # Read Error
        report_headers, report_content = self.generate_file_report(READ_ERROR, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 6
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 12
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 6
        format_errors = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                    severity_id=RULE_SEVERITY_DICT['fatal']).one()
        format_error_count = format_errors.occurrences
        assert format_error_count == 4
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': '',
                'Field Name': 'Formatting Error',
                'Rule Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '2',
                'Rule Label': ''
            },
            {
                'Unique ID': '',
                'Field Name': 'Formatting Error',
                'Rule Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '3',
                'Rule Label': ''
            },
            {
                'Unique ID': '',
                'Field Name': 'Formatting Error',
                'Rule Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '5',
                'Rule Label': ''
            },
            {
                'Unique ID': '',
                'Field Name': 'Formatting Error',
                'Rule Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '7',
                'Rule Label': ''
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # Type Error
        report_headers, report_content = self.generate_file_report(TYPE_ERROR, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 9
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 18
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 9
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert error_count == 1
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 069-013-X-2050-005',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Rule Message': 'The value provided was of the wrong type. Note that all type errors in a line must be'
                                ' fixed before the rest of the validation logic is applied to that line.',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: A',
                'Expected Value': 'This field must be a decimal',
                'Difference': '',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '6',
                'Rule Label': ''
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # Length Error
        report_headers, report_content = self.generate_file_report(LENGTH_ERROR, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 9
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert error_count == 1
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 069-013-X-2050-005',
                'Field Name': 'grossoutlayamountbytas_cpe',
                'Rule Message': 'Value was longer than maximum length for this field.',
                'Value Provided': 'grossoutlayamountbytas_cpe: 35000000000000000000000000',
                'Expected Value': 'Max length: 21',
                'Difference': '',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '6',
                'Rule Label': ''
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # Required Error + SQL Validation
        report_headers, report_content = self.generate_file_report(REQUIRED_ERROR, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 9
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert error_count == 3
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Rule Message': 'This field is required for all submissions but was not provided in this row.',
                'Value Provided': '',
                'Expected Value': '(not blank)',
                'Difference': '',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': ''
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe, obligationsincurredtotalbytas_cpe,'
                              ' unobligatedbalance_cpe',
                'Rule Message': 'StatusOfBudgetaryResourcesTotal_CPE= ObligationsIncurredTotalByTAS_CPE'
                                ' + UnobligatedBalance_CPE',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: , obligationsincurredtotalbytas_cpe: 8.08,'
                                  ' unobligatedbalance_cpe: 2.02',
                'Expected Value': 'StatusOfBudgetaryResourcesTotal_CPE must equal the sum of these elements:'
                                  ' ObligationsIncurredTotalByTAS_CPE + UnobligatedBalance_CPE. The Broker cannot'
                                  ' distinguish which item is incorrect for this rule. Refer to related rule errors'
                                  ' and warnings in this report (rules A15, A22, A23) to distinguish which elements'
                                  ' may be incorrect.',
                'Difference': '-10.10',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': 'A4'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe, totalbudgetaryresources_cpe',
                'Rule Message': 'StatusOfBudgetaryResourcesTotal_CPE = TotalBudgetaryResources_CPE',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: , totalbudgetaryresources_cpe: 10.1',
                'Expected Value': 'StatusOfBudgetaryResourcesTotal_CPE must equal TotalBudgetaryResources_CPE. The'
                                  ' Broker cannot distinguish which side of the equation is correct for this rule.'
                                  ' Refer to related rule errors and warnings in this report (rules A6, A23) to'
                                  ' distinguish which elements may be incorrect.',
                'Difference': '-10.1',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': 'A24'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

        # SQL Validation (with difference)
        report_headers, report_content = self.generate_file_report(RULE_FAILED_ERROR, 'appropriations', warning=False)
        appro_count = self.session.query(Appropriation).filter_by(submission_id=self.submission_id).count()
        assert appro_count == 10
        flex_count = self.session.query(FlexField).filter_by(submission_id=self.submission_id).count()
        assert flex_count == 20
        assert self.validator.job.number_of_rows == 11
        assert self.validator.job.number_of_rows_valid == 10
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert error_count == 0
        assert report_headers == self.validator.report_headers
        # TODO put this back when we put A2 back
        # expected_values = [
        #     {
        #         'Unique ID': 'TAS: 049-2014/2015-0100-000',
        #         'Field Name': 'totalbudgetaryresources_cpe, budgetauthorityappropriatedamount_cpe,'
        #                       ' budgetauthorityunobligatedbalancebroughtforward_fyb,'
        #                       ' adjustmentstounobligatedbalancebroughtforward_cpe, otherbudgetaryresourcesamount_cpe',
        #         'Rule Message': 'TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +'
        #                         ' BudgetAuthorityUnobligatedBalanceBroughtForward_FYB +'
        #                         ' AdjustmentsToUnobligatedBalanceBroughtForward_CPE +'
        #                         ' OtherBudgetaryResourcesAmount_CPE',
        #         'Value Provided': 'totalbudgetaryresources_cpe: 10.1, budgetauthorityappropriatedamount_cpe: 0.01,'
        #                           ' budgetauthorityunobligatedbalancebroughtforward_fyb: 3.03,'
        #                           ' adjustmentstounobligatedbalancebroughtforward_cpe: 2.02,'
        #                           ' otherbudgetaryresourcesamount_cpe: 4.04',
        #         'Expected Value': 'TotalBudgetaryResources_CPE must equal the sum of these elements:'
        #                           ' BudgetAuthorityAppropriatedAmount_CPE +'
        #                           ' BudgetAuthorityUnobligatedBalanceBroughtForward_FYB +'
        #                           ' AdjustmentsToUnobligatedBalanceBroughtForward_CPE +'
        #                           ' OtherBudgetaryResourcesAmount_CPE. The Broker cannot distinguish which item is'
        #                           ' incorrect for this rule. Refer to related rule errors and warnings in this report'
        #                           ' (rules A3, A6, A7, A8, A12) to distinguish which elements may be incorrect.',
        #         'Difference': '1.00',
        #         'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
        #         'Row Number': '10',
        #         'Rule Label': 'A2'
        #     }
        # ]
        # assert report_content == expected_values
        self.cleanup()

        # Ensure total_obligations are being calculated correctly
        self.generate_file_report(AFINANCIAL_FILE, 'award_financial', warning=False)
        totals = self.session.query(TotalObligations).filter_by(submission_id=self.submission_id).one()
        assert totals.total_obligations == 12000.00
        assert totals.total_proc_obligations == 8000.00
        assert totals.total_asst_obligations == 4000.00
        self.cleanup()

    def test_cross_file_warnings(self):
        for chunk_size, parallel, batch_sql in self.CONFIGS:
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'CHUNK_SIZE', chunk_size)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'PARALLEL', parallel)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'BATCH_SQL_VAL_RESULTS',
                                     batch_sql)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validator, 'SQL_VALIDATION_BATCH_SIZE',
                                     chunk_size)
            self.cross_file_warnings()

    def cross_file_warnings(self):
        self.cleanup()

        # Valid
        report_headers, report_content = self.generate_cross_file_report([(CROSS_FILE_A, 'appropriations'),
                                                                          (CROSS_FILE_B, 'program_activity')],
                                                                         warning=True)
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['warning']).count()
        assert error_count == 0
        assert report_headers == self.validator.cross_file_report_headers
        assert len(report_content) == 0
        self.cleanup()

        # SQL Validation
        report_headers, report_content = self.generate_cross_file_report([(INVALID_CROSS_A, 'appropriations'),
                                                                          (INVALID_CROSS_B, 'program_activity')],
                                                                         warning=True)
        warnings = list(self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                    severity_id=RULE_SEVERITY_DICT['warning']).all())
        assert len(warnings) == 3
        assert warnings[0].occurrences == 3
        assert warnings[1].occurrences == 3
        assert warnings[2].occurrences == 3
        assert report_headers == self.validator.cross_file_report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'grossoutlayamountbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'gross_outlay_amount_by_pro_cpe_sum',
                'Rule Message': 'The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the'
                                ' sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the'
                                ' award financial file (B). {This value is the sum of all Gross Outlay Amounts reported'
                                ' in file B, to indicate year-to-date activity by TAS/Subaccount.}',
                'Source Value Provided': 'grossoutlayamountbytas_cpe: 10000',
                'Target Value Provided': 'gross_outlay_amount_by_pro_cpe_sum: 6000',
                'Difference': '4000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '5',
                'Rule Label': 'A18'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'obligationsincurredtotalbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'obligations_incurred_by_pr_cpe_sum',
                'Rule Message': 'The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not'
                                ' equal the negative sum of the corresponding'
                                ' ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).',
                'Source Value Provided': 'obligationsincurredtotalbytas_cpe: 12000',
                'Target Value Provided': 'obligations_incurred_by_pr_cpe_sum: 6000',
                'Difference': '18000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '5',
                'Rule Label': 'A19'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'deobligationsrecoveriesrefundsbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'ussgl487100_downward_adjus_cpe_sum, ussgl497100_downward_adjus_cpe_sum,'
                                     ' ussgl487200_downward_adjus_cpe_sum, ussgl497200_downward_adjus_cpe_sum',
                'Rule Message': 'DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL'
                                ' (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.',
                'Source Value Provided': 'deobligationsrecoveriesrefundsbytas_cpe: 16000',
                'Target Value Provided': 'ussgl487100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl497100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl487200_downward_adjus_cpe_sum: 400,'
                                         ' ussgl497200_downward_adjus_cpe_sum: 2000',
                'Difference': '9600',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '5',
                'Rule Label': 'A35'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'grossoutlayamountbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'gross_outlay_amount_by_pro_cpe_sum',
                'Rule Message': 'The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the'
                                ' sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the'
                                ' award financial file (B). {This value is the sum of all Gross Outlay Amounts reported'
                                ' in file B, to indicate year-to-date activity by TAS/Subaccount.}',
                'Source Value Provided': 'grossoutlayamountbytas_cpe: 10000',
                'Target Value Provided': 'gross_outlay_amount_by_pro_cpe_sum: 6000',
                'Difference': '4000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '10',
                'Rule Label': 'A18'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'obligationsincurredtotalbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'obligations_incurred_by_pr_cpe_sum',
                'Rule Message': 'The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not'
                                ' equal the negative sum of the corresponding'
                                ' ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).',
                'Source Value Provided': 'obligationsincurredtotalbytas_cpe: 12000',
                'Target Value Provided': 'obligations_incurred_by_pr_cpe_sum: 6000',
                'Difference': '18000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '10',
                'Rule Label': 'A19'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'deobligationsrecoveriesrefundsbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'ussgl487100_downward_adjus_cpe_sum, ussgl497100_downward_adjus_cpe_sum,'
                                     ' ussgl487200_downward_adjus_cpe_sum, ussgl497200_downward_adjus_cpe_sum',
                'Rule Message': 'DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL'
                                ' (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.',
                'Source Value Provided': 'deobligationsrecoveriesrefundsbytas_cpe: 16000',
                'Target Value Provided': 'ussgl487100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl497100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl487200_downward_adjus_cpe_sum: 400,'
                                         ' ussgl497200_downward_adjus_cpe_sum: 2000',
                'Difference': '9600',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '10',
                'Rule Label': 'A35'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'grossoutlayamountbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'gross_outlay_amount_by_pro_cpe_sum',
                'Rule Message': 'The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the'
                                ' sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the'
                                ' award financial file (B). {This value is the sum of all Gross Outlay Amounts reported'
                                ' in file B, to indicate year-to-date activity by TAS/Subaccount.}',
                'Source Value Provided': 'grossoutlayamountbytas_cpe: 10000',
                'Target Value Provided': 'gross_outlay_amount_by_pro_cpe_sum: 6000',
                'Difference': '4000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '15',
                'Rule Label': 'A18'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'obligationsincurredtotalbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'obligations_incurred_by_pr_cpe_sum',
                'Rule Message': 'The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not'
                                ' equal the negative sum of the corresponding'
                                ' ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).',
                'Source Value Provided': 'obligationsincurredtotalbytas_cpe: 12000',
                'Target Value Provided': 'obligations_incurred_by_pr_cpe_sum: 6000',
                'Difference': '18000',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '15',
                'Rule Label': 'A19'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'deobligationsrecoveriesrefundsbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'ussgl487100_downward_adjus_cpe_sum, ussgl497100_downward_adjus_cpe_sum,'
                                     ' ussgl487200_downward_adjus_cpe_sum, ussgl497200_downward_adjus_cpe_sum',
                'Rule Message': 'DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL'
                                ' (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.',
                'Source Value Provided': 'deobligationsrecoveriesrefundsbytas_cpe: 16000',
                'Target Value Provided': 'ussgl487100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl497100_downward_adjus_cpe_sum: 2000,'
                                         ' ussgl487200_downward_adjus_cpe_sum: 400,'
                                         ' ussgl497200_downward_adjus_cpe_sum: 2000',
                'Difference': '9600',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '15',
                'Rule Label': 'A35'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

    def test_cross_file_errors(self):
        for chunk_size, parallel, batch_sql in self.CONFIGS:
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'CHUNK_SIZE', chunk_size)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'PARALLEL', parallel)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'BATCH_SQL_VAL_RESULTS',
                                     batch_sql)
            self.monkeypatch.setattr(dataactvalidator.validation_handlers.validator, 'SQL_VALIDATION_BATCH_SIZE',
                                     chunk_size)
            self.cross_file_errors()

    def cross_file_errors(self):
        self.cleanup()

        # Valid
        report_headers, report_content = self.generate_cross_file_report([(CROSS_FILE_A, 'appropriations'),
                                                                          (CROSS_FILE_B, 'program_activity')],
                                                                         warning=False)
        error_count = self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                  severity_id=RULE_SEVERITY_DICT['fatal']).count()
        assert error_count == 0
        assert report_headers == self.validator.cross_file_report_headers
        assert len(report_content) == 0
        self.cleanup()

        # SQL Validation
        report_headers, report_content = self.generate_cross_file_report([(INVALID_CROSS_A, 'appropriations'),
                                                                          (INVALID_CROSS_B, 'program_activity')],
                                                                         warning=False)
        warnings = list(self.session.query(ErrorMetadata).filter_by(job_id=self.val_job.job_id,
                                                                    severity_id=RULE_SEVERITY_DICT['fatal']).all())
        assert len(warnings) == 1
        assert warnings[0].occurrences == 3
        assert report_headers == self.validator.cross_file_report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 019-072-X-0306-000',
                'Source File': 'appropriations',
                'Source Field Name': 'allocationtransferagencyidentifier, agencyidentifier,'
                                     ' beginningperiodofavailability, endingperiodofavailability,'
                                     ' availabilitytypecode, mainaccountcode, subaccountcode',
                'Target File': 'program_activity',
                'Target Field Name': '',
                'Rule Message': 'All TAS values in File A (appropriations) should exist in File B'
                                ' (object class program activity)',
                'Source Value Provided': 'allocationtransferagencyidentifier: 019, agencyidentifier: 072,'
                                         ' beginningperiodofavailability: , endingperiodofavailability: ,'
                                         ' availabilitytypecode: X, mainaccountcode: 0306, subaccountcode: 000',
                'Target Value Provided': '',
                'Difference': '',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '2',
                'Rule Label': 'A30.1'
            },
            {
                'Unique ID': 'TAS: 019-072-X-0306-000',
                'Source File': 'appropriations',
                'Source Field Name': 'allocationtransferagencyidentifier, agencyidentifier,'
                                     ' beginningperiodofavailability, endingperiodofavailability,'
                                     ' availabilitytypecode, mainaccountcode, subaccountcode',
                'Target File': 'program_activity',
                'Target Field Name': '',
                'Rule Message': 'All TAS values in File A (appropriations) should exist in File B'
                                ' (object class program activity)',
                'Source Value Provided': 'allocationtransferagencyidentifier: 019, agencyidentifier: 072,'
                                         ' beginningperiodofavailability: , endingperiodofavailability: ,'
                                         ' availabilitytypecode: X, mainaccountcode: 0306, subaccountcode: 000',
                'Target Value Provided': '',
                'Difference': '',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '7',
                'Rule Label': 'A30.1'
            },
            {
                'Unique ID': 'TAS: 019-072-X-0306-000',
                'Source File': 'appropriations',
                'Source Field Name': 'allocationtransferagencyidentifier, agencyidentifier,'
                                     ' beginningperiodofavailability, endingperiodofavailability,'
                                     ' availabilitytypecode, mainaccountcode, subaccountcode',
                'Target File': 'program_activity',
                'Target Field Name': '',
                'Rule Message': 'All TAS values in File A (appropriations) should exist in File B'
                                ' (object class program activity)',
                'Source Value Provided': 'allocationtransferagencyidentifier: 019, agencyidentifier: 072,'
                                         ' beginningperiodofavailability: , endingperiodofavailability: ,'
                                         ' availabilitytypecode: X, mainaccountcode: 0306, subaccountcode: 000',
                'Target Value Provided': '',
                'Difference': '',
                'Source Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Source Row Number': '12',
                'Rule Label': 'A30.1'
            }
        ]
        assert report_content == expected_values
        self.cleanup()

    def test_validation_parallelize_error(self):
        # Test the parallelize function with a broken call to see if the process is properly cleaned up
        self.monkeypatch.setattr(dataactvalidator.validation_handlers.validationManager, 'MULTIPROCESSING_POOLS', 2)

        # Setting up all the other elements of the validator to simulate the integration test
        self.validator.submission_id = 1
        self.validator.file_type = self.session.query(FileType).filter_by(
            file_type_id=FILE_TYPE_DICT['appropriations']).one()
        self.validator.file_name = APPROP_FILE
        self.setup_csv_record_validation(APPROP_FILE, 'appropriations')
        self.validator.is_fabs = False
        self.validator.reader = CsvReader()
        self.validator.error_list = {}
        self.validator.error_rows = []
        self.validator.total_rows = 1
        self.validator.total_data_rows = 0
        self.validator.short_rows = []
        self.validator.long_rows = []
        self.validator.has_data = False
        self.validator.model = Appropriation

        self.validator.error_file_name = report_file_name(self.validator.submission_id, False,
                                                          self.validator.file_type.name)
        self.validator.error_file_path = ''.join([CONFIG_SERVICES['error_report_path'],
                                                  self.validator.error_file_name])
        self.validator.warning_file_name = report_file_name(self.validator.submission_id, True,
                                                            self.validator.file_type.name)
        self.validator.warning_file_path = ''.join([CONFIG_SERVICES['error_report_path'],
                                                    self.validator.warning_file_name])

        self.validator.fields = self.session.query(FileColumn) \
            .filter(FileColumn.file_id == FILE_TYPE_DICT[self.validator.file_type.name]) \
            .order_by(FileColumn.daims_name.asc()).all()
        self.validator.expected_headers, self.validator.parsed_fields = parse_fields(self.session,
                                                                                     self.validator.fields)
        self.validator.csv_schema = {row.name_short: row for row in self.validator.fields}

        with open(self.validator.error_file_path, 'w', newline='') as error_file, \
                open(self.validator.warning_file_path, 'w', newline='') as warning_file:
            error_csv = csv.writer(error_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            warning_csv = csv.writer(warning_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            error_csv.writerow(self.validator.report_headers)
            warning_csv.writerow(self.validator.report_headers)

        # Finally open the file for loading into the database with baseline validations
        self.validator.filename = self.validator.reader.get_filename(None, None, self.validator.file_name)
        self.validator.reader.open_file(None, None, self.validator.file_name, self.validator.fields, None,
                                        self.validator.get_file_name(self.validator.error_file_name),
                                        self.validator.daims_to_short_dict[self.validator.file_type.file_type_id],
                                        self.validator.short_to_daims_dict[self.validator.file_type.file_type_id],
                                        is_local=self.validator.is_local)

        # Going back to reprocess the header row
        self.validator.reader.file.seek(0)
        reader_obj = pd.read_csv(self.validator.reader.file, dtype=str, delimiter=',', error_bad_lines=False,
                                 na_filter=False, chunksize=2, warn_bad_lines=False)
        # Setting this outside of reader/file type objects which may not be used during processing
        self.validator.flex_fields = ['flex_field_a', 'flex_field_b']
        self.validator.header_dict = self.validator.reader.header_dict
        self.validator.file_type_name = self.validator.file_type.name
        self.validator.file_type_id = self.validator.file_type.file_type_id
        self.validator.job_id = 2

        # Making a broken list of chunks (one that should process fine, another with an error, another fine)
        # This way we can tell that the latter chunks processed later are ignored due to the error
        normal_chunks = list(reader_obj)
        broken_chunks = [normal_chunks[0], 'BREAK', normal_chunks[1], normal_chunks[2]]

        with self.assertRaises(Exception) as val_except:
            # making the reader object a list of strings instead, causing the inner function to break
            self.validator.parallel_data_loading(self.session, broken_chunks)
        self.assertTrue(type(val_except.exception) == AttributeError)
        self.assertTrue(str(val_except.exception) == "'str' object has no attribute 'empty'")

        # Check to see the processes are killed
        job = ps.Process(os.getpid())
        assert len(job.children(recursive=True)) == 0
