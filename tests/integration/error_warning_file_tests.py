import os
import csv
import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_SERVICES
from dataactcore.models.domainModels import concat_tas_dict
from dataactcore.models.lookups import (FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT)
from dataactcore.models.jobModels import Submission
from dataactcore.models.userModel import User
from dataactcore.models.errorModels import ErrorMetadata
from dataactcore.models.stagingModels import Appropriation, ObjectClassProgramActivity
from dataactvalidator.health_check import create_app
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactbroker.handlers.fileHandler import report_file_name

from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from tests.integration.baseTestValidator import BaseTestValidator
from tests.integration.integration_test_helper import insert_submission, insert_job

FILES_DIR = os.path.join('tests', 'integration', 'data')

# Valid Files
APPROP_FILE = os.path.join(FILES_DIR, 'appropValid.csv')
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

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(ErrorWarningTests, cls).setUpClass()

        logging.getLogger('dataactcore').setLevel(logging.ERROR)
        logging.getLogger('dataactvalidator').setLevel(logging.ERROR)

        with create_app().app_context():
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
                              internal_start_date='01-01-2000')
            tas2 = TASFactory(account_num=2, allocation_transfer_agency=None, agency_identifier='019',
                              beginning_period_of_availa='2016', ending_period_of_availabil='2016',
                              availability_type_code=None, main_account_code='0113', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas3 = TASFactory(account_num=3, allocation_transfer_agency=None, agency_identifier='028',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='0406', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas4 = TASFactory(account_num=4, allocation_transfer_agency=None, agency_identifier='028',
                              beginning_period_of_availa='2010', ending_period_of_availabil='2011',
                              availability_type_code=None, main_account_code='0406', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas5 = TASFactory(account_num=5, allocation_transfer_agency='069', agency_identifier='013',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='2050', sub_account_code='005',
                              internal_start_date='01-01-2000')
            tas6 = TASFactory(account_num=6, allocation_transfer_agency='028', agency_identifier='028',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='8007', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas7 = TASFactory(account_num=7, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa=None, ending_period_of_availabil=None,
                              availability_type_code='X', main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas8 = TASFactory(account_num=8, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa='2010', ending_period_of_availabil='2011',
                              availability_type_code=None, main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000')
            tas9 = TASFactory(account_num=9, allocation_transfer_agency=None, agency_identifier='049',
                              beginning_period_of_availa='2014', ending_period_of_availabil='2015',
                              availability_type_code=None, main_account_code='0100', sub_account_code='000',
                              internal_start_date='01-01-2000')
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
        self.val_job.filename = file
        self.val_job.file_type_id = FILE_TYPE_DICT[file_type]
        self.val_job.job_status_id = JOB_STATUS_DICT['ready']
        self.val_job.job_type_id = JOB_TYPE_DICT['csv_record_validation']
        self.session.commit()

    def setup_validation(self):
        self.val_job.filename = None
        self.val_job.file_type_id = None
        self.val_job.job_status_id = JOB_STATUS_DICT['ready']
        self.val_job.job_type_id = JOB_TYPE_DICT['validation']
        self.session.commit()

    def get_report_content(self, report_path):
        report_content = []
        report_headers = None
        with open(report_path, 'r') as report_csv:
            reader = csv.DictReader(report_csv)
            for row in reader:
                report_content.append(row)
            report_headers = reader.fieldnames
        return report_headers, report_content

    def generate_file_report(self, file, file_type, warning=False, ignore_error=False, cleanup=True):
        self.setup_csv_record_validation(file, file_type)
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(file_type, warning=warning)
        report_content = self.get_report_content(report_path)
        if cleanup:
            self.cleanup()
        return report_content

    def generate_cross_file_report(self, cross_files, warning=False, ignore_error=False, cleanup=True):
        cross_types = []
        for cross_file in cross_files:
            cross_types.append(cross_file[1])
            self.generate_file_report(cross_file[0], cross_file[1], warning=warning, ignore_error=ignore_error,
                                      cleanup=False)

        self.setup_validation()
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(cross_types[0], cross_type=cross_types[1], warning=warning)
        report_content = self.get_report_content(report_path)
        if cleanup:
            self.cleanup()
        return report_content

    def cleanup(self):
        new_reports = set(os.listdir(CONFIG_SERVICES['error_report_path'])) - self.original_reports
        for new_report in new_reports:
            os.remove(os.path.join(CONFIG_SERVICES['error_report_path'], new_report))
        self.session.query(Appropriation).delete(synchronize_session='fetch')
        self.session.query(ObjectClassProgramActivity).delete(synchronize_session='fetch')
        self.session.query(ErrorMetadata).delete(synchronize_session='fetch')
        self.session.commit()

    def test_single_file_warnings(self):
        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE, 'appropriations', warning=True)
        assert report_headers == self.validator.report_headers
        assert len(report_content) == 0

        # Length Error
        report_headers, report_content = self.generate_file_report(LENGTH_ERROR, 'appropriations', warning=True)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 069-013-X-2050-005',
                'Field Name': 'grossoutlayamountbytas_cpe',
                'Error Message': 'Value was longer than maximum length for this field.',
                'Value Provided': 'grossoutlayamountbytas_cpe: 35000000000000000000000000',
                'Expected Value': 'Max length: 21',
                'Difference': '',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '6',
                'Rule Label': ''
            }
        ]
        assert report_content == expected_values

        # SQL Validation
        report_headers, report_content = self.generate_file_report(RULE_FAILED_WARNING, 'appropriations', warning=True)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 028-2010/2011-0406-000',
                'Field Name': 'budgetauthorityunobligatedbalancebroughtforward_fyb',
                'Error Message': 'All the elements that have FYB in file A are expected in the first submission'
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

    def test_single_file_errors(self):
        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE, 'appropriations', warning=False)
        assert report_headers == self.validator.report_headers
        assert len(report_content) == 0

        # Header Error
        report_headers, report_content = self.generate_file_report(HEADER_ERROR, 'appropriations', warning=False,
                                                                   ignore_error=True)
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

        # Read Error
        report_headers, report_content = self.generate_file_report(READ_ERROR, 'appropriations', warning=False)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': '',
                'Field Name': 'Formatting Error',
                'Error Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Difference': '',
                'Flex Field': '',
                'Row Number': '6',
                'Rule Label': ''
            }
        ]
        assert report_content == expected_values

        # Type Error
        report_headers, report_content = self.generate_file_report(TYPE_ERROR, 'appropriations', warning=False)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 069-013-X-2050-005',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Error Message': 'The value provided was of the wrong type. Note that all type errors in a line must be'
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

        # Required Error + SQL Validation
        report_headers, report_content = self.generate_file_report(REQUIRED_ERROR, 'appropriations', warning=False)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Error Message': 'This field is required for all submissions but was not provided in this row.',
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
                'Error Message': 'StatusOfBudgetaryResourcesTotal_CPE= ObligationsIncurredTotalByTAS_CPE'
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
                'Error Message': 'StatusOfBudgetaryResourcesTotal_CPE = TotalBudgetaryResources_CPE',
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

        # SQL Validation (with difference)
        report_headers, report_content = self.generate_file_report(RULE_FAILED_ERROR, 'appropriations', warning=False)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 049-2014/2015-0100-000',
                'Field Name': 'totalbudgetaryresources_cpe, budgetauthorityappropriatedamount_cpe,'
                              ' budgetauthorityunobligatedbalancebroughtforward_fyb,'
                              ' adjustmentstounobligatedbalancebroughtforward_cpe, otherbudgetaryresourcesamount_cpe',
                'Error Message': 'TotalBudgetaryResources_CPE = BudgetAuthorityAppropriatedAmount_CPE +'
                                 ' BudgetAuthorityUnobligatedBalanceBroughtForward_FYB +'
                                 ' AdjustmentsToUnobligatedBalanceBroughtForward_CPE +'
                                 ' OtherBudgetaryResourcesAmount_CPE',
                'Value Provided': 'totalbudgetaryresources_cpe: 10.1, budgetauthorityappropriatedamount_cpe: 0.01,'
                                  ' budgetauthorityunobligatedbalancebroughtforward_fyb: 3.03,'
                                  ' adjustmentstounobligatedbalancebroughtforward_cpe: 2.02,'
                                  ' otherbudgetaryresourcesamount_cpe: 4.04',
                'Expected Value': 'TotalBudgetaryResources_CPE must equal the sum of these elements:'
                                  ' BudgetAuthorityAppropriatedAmount_CPE +'
                                  ' BudgetAuthorityUnobligatedBalanceBroughtForward_FYB +'
                                  ' AdjustmentsToUnobligatedBalanceBroughtForward_CPE +'
                                  ' OtherBudgetaryResourcesAmount_CPE. The Broker cannot distinguish which item is'
                                  ' incorrect for this rule. Refer to related rule errors and warnings in this report'
                                  ' (rules A3, A6, A7, A8, A12) to distinguish which elements may be incorrect.',
                'Difference': '1.00',
                'Flex Field': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '10',
                'Rule Label': 'A2'
            }
        ]
        assert report_content == expected_values

    def test_cross_file_warnings(self):
        # Valid
        report_headers, report_content = self.generate_cross_file_report([(CROSS_FILE_A, 'appropriations'),
                                                                          (CROSS_FILE_B, 'program_activity')],
                                                                         warning=True)
        assert report_headers == self.validator.cross_file_report_headers
        assert len(report_content) == 0

        # SQL Validation
        report_headers, report_content = self.generate_cross_file_report([(INVALID_CROSS_A, 'appropriations'),
                                                                          (INVALID_CROSS_B, 'program_activity')],
                                                                         warning=True)
        assert report_headers == self.validator.cross_file_report_headers
        expected_values = [
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'grossoutlayamountbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'gross_outlay_amount_by_pro_cpe_sum',
                'Error Message': 'The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal'
                                 ' the sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in'
                                 ' the award financial file (B).',
                'Source Values Provided': 'grossoutlayamountbytas_cpe: 10000',
                'Target Values Provided': 'gross_outlay_amount_by_pro_cpe_sum: 6000',
                'Difference': '4000',
                'Source Flex Field': '',
                'Source Row Number': '5',
                'Rule Label': 'A18'
            },
            {
                'Unique ID': 'TAS: 019-2016/2016-0113-000',
                'Source File': 'appropriations',
                'Source Field Name': 'obligationsincurredtotalbytas_cpe',
                'Target File': 'program_activity',
                'Target Field Name': 'obligations_incurred_by_pr_cpe_sum',
                'Error Message': 'The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not'
                                 ' equal the negative sum of the corresponding'
                                 ' ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).',
                'Source Values Provided': 'obligationsincurredtotalbytas_cpe: 12000',
                'Target Values Provided': 'obligations_incurred_by_pr_cpe_sum: -6000',
                'Difference': '18000',
                'Source Flex Field': '',
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
                'Error Message': 'DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL'
                                 ' (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.',
                'Source Values Provided': 'deobligationsrecoveriesrefundsbytas_cpe: 16000',
                'Target Values Provided': 'ussgl487100_downward_adjus_cpe_sum: 2000,'
                                          ' ussgl497100_downward_adjus_cpe_sum: 2000,'
                                          ' ussgl487200_downward_adjus_cpe_sum: 400,'
                                          ' ussgl497200_downward_adjus_cpe_sum: 2000',
                'Difference': '9600',
                'Source Flex Field': '',
                'Source Row Number': '5',
                'Rule Label': 'A35'
            }
        ]
        assert report_content == expected_values

    def test_cross_file_errors(self):
        # Valid
        report_headers, report_content = self.generate_cross_file_report([(CROSS_FILE_A, 'appropriations'),
                                                                          (CROSS_FILE_B, 'program_activity')],
                                                                         warning=False)
        assert report_headers == self.validator.cross_file_report_headers
        assert len(report_content) == 0

        # SQL Validation
        report_headers, report_content = self.generate_cross_file_report([(INVALID_CROSS_A, 'appropriations'),
                                                                          (INVALID_CROSS_B, 'program_activity')],
                                                                         warning=False)
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
                'Error Message': 'All TAS values in File A (appropriations) should exist in File B'
                                 ' (object class program activity)',
                'Source Values Provided': 'allocationtransferagencyidentifier: 019, agencyidentifier: 072,'
                                          ' beginningperiodofavailability: , endingperiodofavailability: ,'
                                          ' availabilitytypecode: X, mainaccountcode: 0306, subaccountcode: 000',
                'Target Values Provided': '',
                'Difference': '',
                'Source Flex Field': '',
                'Source Row Number': '2',
                'Rule Label': 'A30.1'
            }
        ]
        assert report_content == expected_values
