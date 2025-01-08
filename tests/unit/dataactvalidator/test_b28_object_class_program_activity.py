from tests.unit.dataactcore.factories.domain import ProgramActivityPARKFactory
from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'b28_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'program_activity_reporting_key', 'uniqueid_TAS', 'uniqueid_ObjectClass'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """
        Should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined in the
        OMB’s Program Activity Mapping File. Ignore rule for $0 rows
    """

    park = ProgramActivityPARKFactory(agency_id='123', allocation_transfer_id=None, main_account_number='0001',
                                      sub_account_number=None, park_code='abcd')
    park_sub = ProgramActivityPARKFactory(agency_id='123', allocation_transfer_id=None, main_account_number='0002',
                                          sub_account_number='001', park_code='AbCdEf')

    # Main account code that has no sub in PARK, sub ignored
    op1 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0001', sub_account_code='123',
                                            program_activity_reporting_key='aBcD')
    # Matching main and sub accounts
    op2 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0002', sub_account_code='001',
                                            program_activity_reporting_key='aBcDeF')
    # Not matching main account but all 0s in USSGLs so ignored
    op3 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0002', sub_account_code='001',
                                            program_activity_reporting_key='XYZ',
                                            ussgl480100_undelivered_or_fyb=0, ussgl480100_undelivered_or_cpe=0,
                                            ussgl480110_rein_undel_ord_cpe=0, ussgl480200_undelivered_or_cpe=0,
                                            ussgl480200_undelivered_or_fyb=0, ussgl483100_undelivered_or_cpe=0,
                                            ussgl483200_undelivered_or_cpe=0, ussgl487100_downward_adjus_cpe=0,
                                            ussgl487200_downward_adjus_cpe=0, ussgl488100_upward_adjustm_cpe=0,
                                            ussgl488200_upward_adjustm_cpe=0, ussgl490100_delivered_orde_fyb=0,
                                            ussgl490100_delivered_orde_cpe=0, ussgl490110_rein_deliv_ord_cpe=0,
                                            ussgl490200_delivered_orde_cpe=0, ussgl490800_authority_outl_fyb=0,
                                            ussgl490800_authority_outl_cpe=0, ussgl493100_delivered_orde_cpe=0,
                                            ussgl497100_downward_adjus_cpe=0, ussgl497200_downward_adjus_cpe=0,
                                            ussgl498100_upward_adjustm_cpe=0, ussgl498200_upward_adjustm_cpe=0)
    # Ignored for NULL PARK
    op4 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0002', sub_account_code='003',
                                            program_activity_reporting_key=None)
    assert number_of_errors(_FILE, database, models=[op1, op2, op3, op4, park, park_sub]) == 0


def test_failure(database):
    """
        Failure should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined
        in the OMB’s Program Activity Mapping File. Ignore rule for $0 rows
    """

    park = ProgramActivityPARKFactory(agency_id='123', allocation_transfer_id=None, main_account_number='0001',
                                      sub_account_number=None, park_code='ABCD')
    park_sub = ProgramActivityPARKFactory(agency_id='123', allocation_transfer_id=None, main_account_number='0002',
                                          sub_account_number='001', park_code='ABCDEF')
    # Non-matching sub account code
    op1 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0002', sub_account_code='123',
                                            program_activity_reporting_key='ABCDEF')
    # Non-matching TAS even though PARK exists
    op2 = ObjectClassProgramActivityFactory(agency_identifier='321', allocation_transfer_agency=None,
                                            main_account_code='0001', sub_account_code='123',
                                            program_activity_reporting_key='ABCD')
    # PARK that doesn't exist even though TAS does
    op3 = ObjectClassProgramActivityFactory(agency_identifier='123', allocation_transfer_agency=None,
                                            main_account_code='0001', sub_account_code='123',
                                            program_activity_reporting_key='ABCDE')

    assert number_of_errors(_FILE, database, models=[op1, op2, op3, park, park_sub]) == 3
