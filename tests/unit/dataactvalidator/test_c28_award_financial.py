from dataactcore.models.stagingModels import AwardFinancial
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'c28_award_financial'


def test_column_headers(database):
    expected_subset = {'row_number', 'beginning_period_of_availa', 'ending_period_of_availabil',
                       'agency_identifier', 'allocation_transfer_agency', 'availability_type_code',
                       'main_account_code', 'sub_account_code', 'object_class', 'program_activity_code',
                       'by_direct_reimbursable_fun', 'disaster_emergency_fund_code', 'fain', 'uri', 'piid',
                       'parent_award_id', 'uniqueid_TAS', 'uniqueid_ProgramActivityCode',
                       'uniqueid_ProgramActivityName', 'uniqueid_ObjectClass',
                       'uniqueid_ByDirectReimbursableFundingSource', 'uniqueid_DisasterEmergencyFundCode',
                       'uniqueid_FAIN', 'uniqueid_URI', 'uniqueid_PIID', 'uniqueid_ParentAwardId'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """
        Tests the combination of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID
        in File C (award financial) should be unique for USSGL-related balances.
    """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='2',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af3 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='2', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af4 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='2',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af5 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='2', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af6 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='2',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af7 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='2', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af8 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='2', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af9 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='2',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af10 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='2', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='1', uri='1', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    af11 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='m',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='1', uri='1', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    af12 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='d', disaster_emergency_fund_code='n',
                          fain='1', uri='1', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    # Same values but a different DEFC
    af13 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='m',
                          fain='1', uri='1', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    af14 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='2', uri='1', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    af15 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='1', uri='2', piid='1', parent_award_id='1',
                          transaction_obligated_amou=None)

    af16 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='1', uri='1', piid='2', parent_award_id='1',
                          transaction_obligated_amou=None)

    af17 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                          ending_period_of_availabil='1', agency_identifier='1',
                          allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1',
                          program_activity_code='1', program_activity_name='n',
                          by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                          fain='1', uri='1', piid='1', parent_award_id='2',
                          transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2, af3, af4, af5, af6, af7, af8, af9, af10, af11,
                                                     af12, af13, af14, af15, af16, af17]) == 0


def test_ignore_toa(database):
    """
        Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID
        in File C (award financial) are not unique for USSGL-related balances, ignoring when TOA is not NULL
    """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         availability_type_code='1', main_account_code='1', sub_account_code='1',
                         object_class='1', program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=0)

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         availability_type_code='1', main_account_code='1', sub_account_code='1',
                         object_class='1', program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=0)

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 0


def test_optionals(database):
    """
        Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID
        in File C (award financial) are not unique for USSGL-related balances, while omitting an optional field to
        check that there is still a match.
    """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         availability_type_code='1', main_account_code='1', sub_account_code='1',
                         object_class='1', program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         availability_type_code='1', main_account_code='1', sub_account_code='1',
                         object_class='1', program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 1


def test_failure(database):
    """
        Tests that all combinations of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID
        in File C (award financial) are not unique for USSGL-related balances.
    """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='n',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1',
                         ending_period_of_availabil='1', agency_identifier='1',
                         allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1',
                         program_activity_code='1', program_activity_name='n',
                         by_direct_reimbursable_fun='r', disaster_emergency_fund_code='N',
                         fain='1', uri='1', piid='1', parent_award_id='1',
                         transaction_obligated_amou=None)

    assert number_of_errors(_FILE, database, models=[af1, af2]) == 1
