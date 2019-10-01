import os
import csv

from dataactcore.interfaces.db import GlobalDB
from dataactcore.config import CONFIG_SERVICES
from dataactcore.models.jobModels import Submission
from dataactcore.models.domainModels import concat_tas_dict
from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
from dataactvalidator.health_check import create_app
from dataactvalidator.validation_handlers.validationManager import ValidationManager
from dataactcore.models.lookups import (FILE_TYPE_DICT, JOB_TYPE_DICT, JOB_STATUS_DICT)
from dataactbroker.handlers.fileHandler import report_file_name

from tests.integration.baseTestValidator import BaseTestValidator
from tests.integration.integration_test_helper import insert_submission, insert_job

FILES_DIR = os.path.join('tests', 'integration', 'data')

# Valid Files
APPROP_FILE_T = os.path.join(FILES_DIR, 'appropValid.csv')
CROSS_FILE_A = os.path.join(FILES_DIR, 'cross_file_A.csv')
CROSS_FILE_B = os.path.join(FILES_DIR, 'cross_file_B.csv')

# Invalid Files
HEADER_ERROR = os.path.join(FILES_DIR, 'appropHeaderError.csv')
READ_ERROR = os.path.join(FILES_DIR, 'appropReadError.csv')
LENGTH_ERROR = os.path.join(FILES_DIR, 'appropLengthError.csv')
TYPE_ERROR = os.path.join(FILES_DIR, 'appropTypeError.csv')
REQUIRED_ERROR = os.path.join(FILES_DIR, 'appropRequiredError.csv')
RULE_FAILED_WARNING = os.path.join(FILES_DIR, 'appropInvalidWarning.csv')
INVALID_CROSS_A = os.path.join(FILES_DIR, 'invalid_cross_file_A.csv')
INVALID_CROSS_B = os.path.join(FILES_DIR, 'invalid_cross_file_B.csv')


class ErrorWarningTests(BaseTestValidator):
    """
    Overall integration tests for error/warning reports.

    For each file type (single-file, cross-file, errors, warnings), test if each has
        - the correct structure
        - each column's content is correct after testing each possible type of error:
            - formatting
            - length
            - types
            - required/optional
            - SQL validation
    """

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(ErrorWarningTests, cls).setUpClass()

        with create_app().app_context():
            # get the submission test users
            sess = GlobalDB.db().session
            cls.session = sess
            cls.admin_user_id = 1

            cls.validator = ValidationManager(directory=CONFIG_SERVICES['error_report_path'])

            # Just have one valid submission and then keep on reloading files
            cls.submission_id = insert_submission(sess, cls.admin_user_id, cgac_code='SYS', start_date='01/2001',
                                                  end_date='03/2001', is_quarter=True)
            cls.submission = sess.query(Submission).filter_by(submission_id=cls.submission_id).one()
            cls.val_job = insert_job(cls.session, FILE_TYPE_DICT['appropriations'], JOB_STATUS_DICT['ready'],
                                     JOB_TYPE_DICT['csv_record_validation'], cls.submission_id,
                                     filename=JOB_TYPE_DICT['csv_record_validation'])

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
                                 agency_identifier='072', beginning_period_of_availa=None,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0306', sub_account_code='000', period=6, fiscal_year=2001)
            gtas2 = SF133Factory(tas=concat_tas_dict(tas2.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='019', beginning_period_of_availa='2016',
                                 ending_period_of_availabil='2016', availability_type_code=None,
                                 main_account_code='0113', sub_account_code='000', period=6, fiscal_year=2001)
            gtas3 = SF133Factory(tas=concat_tas_dict(tas3.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='028', beginning_period_of_availa=None,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0406', sub_account_code='000', period=6, fiscal_year=2001)
            gtas4 = SF133Factory(tas=concat_tas_dict(tas4.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='028', beginning_period_of_availa='2010',
                                 ending_period_of_availabil='2011', availability_type_code=None,
                                 main_account_code='0406', sub_account_code='000', period=6, fiscal_year=2001)
            gtas5 = SF133Factory(tas=concat_tas_dict(tas5.component_dict()), allocation_transfer_agency='069',
                                 agency_identifier='013', beginning_period_of_availa=None,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='2050', sub_account_code='005', period=6, fiscal_year=2001)
            gtas6 = SF133Factory(tas=concat_tas_dict(tas6.component_dict()), allocation_transfer_agency='028',
                                 agency_identifier='028', beginning_period_of_availa=None,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='8007', sub_account_code='000', period=6, fiscal_year=2001)
            gtas7 = SF133Factory(tas=concat_tas_dict(tas7.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa=None,
                                 ending_period_of_availabil=None, availability_type_code='X',
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas8 = SF133Factory(tas=concat_tas_dict(tas8.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa='2010',
                                 ending_period_of_availabil='2011', availability_type_code=None,
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas9 = SF133Factory(tas=concat_tas_dict(tas9.component_dict()), allocation_transfer_agency=None,
                                 agency_identifier='049', beginning_period_of_availa='2014',
                                 ending_period_of_availabil='2015', availability_type_code=None,
                                 main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            gtas10 = SF133Factory(tas=concat_tas_dict(tas10.component_dict()), allocation_transfer_agency=None,
                                  agency_identifier='049', beginning_period_of_availa='2015',
                                  ending_period_of_availabil='2016', availability_type_code=None,
                                  main_account_code='0100', sub_account_code='000', period=6, fiscal_year=2001)
            sess.add_all([gtas1, gtas2, gtas3, gtas4, gtas5, gtas6, gtas7, gtas8, gtas9, gtas10])
            sess.flush()

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

    def generate_file_report(self, file, file_type, warning=False, ignore_error=False):
        self.setup_csv_record_validation(file, file_type)
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(file_type, warning=warning)
        return self.get_report_content(report_path)

    def generate_cross_file_report(self, cross_files, warning=False, ignore_error=False):
        cross_types = []
        for cross_file in cross_files:
            cross_types.append(cross_file[1])
            self.generate_file_report(cross_file[0], cross_file[1], warning=warning, ignore_error=ignore_error)

        self.setup_validation()
        if ignore_error:
            try:
                self.validator.validate_job(self.val_job.job_id)
            except:
                pass
        else:
            self.validator.validate_job(self.val_job.job_id)
        report_path = self.get_report_path(cross_types[0], cross_type=cross_types[1], warning=warning)
        return self.get_report_content(report_path)

    def test_single_file_warnings(self):
        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE_T, 'appropriations', warning=True)
        assert report_headers == self.validator.report_headers
        assert len(report_content) == 0

        # Length Error
        report_headers, report_content = self.generate_file_report(LENGTH_ERROR, 'appropriations', warning=True)
        assert report_headers == self.validator.report_headers
        expected_values = [
            {
                'Field Name': 'grossoutlayamountbytas_cpe',
                'Error Message': 'Value was longer than maximum length for this field.',
                'Value Provided': 'grossoutlayamountbytas_cpe: 35000000000000000000000000',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
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
                'Field Name': 'budgetauthorityunobligatedbalancebroughtforward_fyb',
                'Error Message': 'All the elements that have FYB in file A are expected in the first submission'
                                 ' for a fiscal year',
                'Value Provided': 'budgetauthorityunobligatedbalancebroughtforward_fyb: None',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '5',
                'Rule Label': 'A16.1'
            }
        ]
        assert report_content == expected_values

    def test_single_file_errors(self):
        # Valid
        report_headers, report_content = self.generate_file_report(APPROP_FILE_T, 'appropriations', warning=False)
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
                'Field Name': 'Formatting Error',
                'Error Message': 'Could not parse this record correctly.',
                'Value Provided': '',
                'Expected Value': '',
                'Flex Fields': '',
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
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Error Message': 'The value provided was of the wrong type. Note that all type errors in a line must be'
                                 ' fixed before the rest of the validation logic is applied to that line.',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: A',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
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
                'Field Name': 'statusofbudgetaryresourcestotal_cpe',
                'Error Message': 'This field is required for all submissions but was not provided in this row.',
                'Value Provided': '',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': ''
            },
            {
                'Field Name': 'statusofbudgetaryresourcestotal_cpe, obligationsincurredtotalbytas_cpe,'
                              ' unobligatedbalance_cpe',
                'Error Message': 'StatusOfBudgetaryResourcesTotal_CPE= ObligationsIncurredTotalByTAS_CPE'
                                 ' + UnobligatedBalance_CPE',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: None, obligationsincurredtotalbytas_cpe: 8.08,'
                                  ' unobligatedbalance_cpe: 2.02',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': 'A4'
            },
            {
                'Field Name': 'statusofbudgetaryresourcestotal_cpe, totalbudgetaryresources_cpe',
                'Error Message': 'StatusOfBudgetaryResourcesTotal_CPE = TotalBudgetaryResources_CPE',
                'Value Provided': 'statusofbudgetaryresourcestotal_cpe: None, totalbudgetaryresources_cpe: 10.1',
                'Expected Value': '',
                'Flex Fields': 'flex_field_a: FLEX_A, flex_field_b: FLEX_B',
                'Row Number': '3',
                'Rule Label': 'A24'
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
                'Source File': 'appropriations',
                'Target File': 'program_activity',
                'Field names': 'allocationtransferagencyidentifier, agencyidentifier, beginningperiodofavailability,'
                               ' endingperiodofavailability, availabilitytypecode, mainaccountcode, subaccountcode,'
                               ' grossoutlayamountbytas_cpe, gross_outlay_amount_by_pro_cpe_sum, flex_field_a_fileb,'
                               ' flex_field_b_fileb',
                'Error message': 'The GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal'
                                 ' the sum of the corresponding GrossOutlayAmountByProgramObjectClass_CPE values in'
                                 ' the award financial file (B).',
                'Values provided': 'allocationtransferagencyidentifier: None, agencyidentifier: 019,'
                                   ' beginningperiodofavailability: 2016, endingperiodofavailability: 2016,'
                                   ' availabilitytypecode: None, mainaccountcode: 0113, subaccountcode: 000,'
                                   ' grossoutlayamountbytas_cpe: 10000, gross_outlay_amount_by_pro_cpe_sum: 6000,'
                                   ' flex_field_a_fileb: FLEX_A, flex_field_b_fileb: FLEX_B',
                'Row number': '5',
                'Rule label': 'A18'
            },
            {
                'Source File': 'appropriations',
                'Target File': 'program_activity',
                'Field names': 'allocationtransferagencyidentifier, agencyidentifier, beginningperiodofavailability,'
                               ' endingperiodofavailability, availabilitytypecode, mainaccountcode, subaccountcode,'
                               ' obligationsincurredtotalbytas_cpe, obligations_incurred_by_pr_cpe_sum,'
                               ' flex_field_a_fileb, flex_field_b_fileb',
                'Error message': 'The ObligationsIncurredTotalByTAS_CPE amount in the appropriation file (A) does not'
                                 ' equal the negative sum of the corresponding'
                                 ' ObligationsIncurredByProgramObjectClass_CPE values in the award financial file (B).',
                'Values provided': 'allocationtransferagencyidentifier: None, agencyidentifier: 019,'
                                   ' beginningperiodofavailability: 2016, endingperiodofavailability: 2016,'
                                   ' availabilitytypecode: None, mainaccountcode: 0113, subaccountcode: 000,'
                                   ' obligationsincurredtotalbytas_cpe: 12000,'
                                   ' obligations_incurred_by_pr_cpe_sum: -6000, flex_field_a_fileb: FLEX_A,'
                                   ' flex_field_b_fileb: FLEX_B',
                'Row number': '5',
                'Rule label': 'A19'
            },
            {
                'Source File': 'appropriations',
                'Target File': 'program_activity',
                'Field names': 'deobligationsrecoveriesrefundsbytas_cpe, ussgl487100_downward_adjus_cpe_sum,'
                               ' ussgl497100_downward_adjus_cpe_sum, ussgl487200_downward_adjus_cpe_sum,'
                               ' ussgl497200_downward_adjus_cpe_sum, flex_field_a_fileb, flex_field_b_fileb',
                'Error message': 'DeobligationsRecoveriesRefundsByTAS_CPE in File A should equal USSGL'
                                 ' (4871_CPE+ 4971_CPE+ 4872_CPE+ 4972_CPE) for the TAS in File B.',
                'Values provided': 'deobligationsrecoveriesrefundsbytas_cpe: 16000,'
                                   ' ussgl487100_downward_adjus_cpe_sum: 2000,'
                                   ' ussgl497100_downward_adjus_cpe_sum: 2000, ussgl487200_downward_adjus_cpe_sum: 400,'
                                   ' ussgl497200_downward_adjus_cpe_sum: 2000, flex_field_a_fileb: FLEX_A,'
                                   ' flex_field_b_fileb: FLEX_B',
                'Row number': '5',
                'Rule label': 'A35'
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
                'Source File': 'appropriations',
                'Target File': 'program_activity',
                'Field names': 'allocationtransferagencyidentifier, agencyidentifier, beginningperiodofavailability,'
                               ' endingperiodofavailability, availabilitytypecode, mainaccountcode, subaccountcode,'
                               ' flex_field_a_fileb, flex_field_b_fileb',
                'Error message': 'All TAS values in File A (appropriations) should exist in File B'
                                 ' (object class program activity)',
                'Values provided': 'allocationtransferagencyidentifier: 019, agencyidentifier: 072,'
                                   ' beginningperiodofavailability: None, endingperiodofavailability: None,'
                                   ' availabilitytypecode: X, mainaccountcode: 0306, subaccountcode: 000,'
                                   ' flex_field_a_fileb: FLEX_A, flex_field_b_fileb: FLEX_B',
                'Row number': '2',
                'Rule label': 'A30.1'
            }
        ]
        assert report_content == expected_values
