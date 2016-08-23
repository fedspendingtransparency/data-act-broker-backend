from dataactcore.models.stagingModels import AwardFinancial
from tests.unit.dataactvalidator.utils import number_of_errors


_FILE = 'c19_award_financial'


def test_success(database):
    """ Tests that the combination of TAS / object class / program activity code (if supplied) / award id components /
    transaction obligated amount in File C (award financial) are unique. """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                        agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                        main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                        parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='2', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af3 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='2',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af4 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='2', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af5 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='2', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af6 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='2',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af7 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='2', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af8 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='2', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af9 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='2', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af10 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='2',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af11 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='2', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af12 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='2', uri='1', fain='1', transaction_obligated_amou='1')

    af13 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='2', fain='1', transaction_obligated_amou='1')

    af14 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='2', transaction_obligated_amou='1')

    af15 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                          agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                          main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                          parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='2')

    assert number_of_errors(_FILE, database.stagingDb, models=[af1, af2, af3, af4, af5, af6, af7, af8,
                                                               af9, af10, af11, af12, af13, af14, af15]) == 0


def test_failure(database):
    """ Tests that the combination of TAS / object class / program activity code (if supplied) / award id components /
    transaction obligated amount in File C (award financial) are not unique. """

    af1 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    af2 = AwardFinancial(job_id=1, row_number=1, beginning_period_of_availa='1', ending_period_of_availabil='1',
                         agency_identifier='1', allocation_transfer_agency='1', availability_type_code='1',
                         main_account_code='1', sub_account_code='1', object_class='1', program_activity_code='1',
                         parent_award_id='1', piid='1', uri='1', fain='1', transaction_obligated_amou='1')

    assert number_of_errors(_FILE, database.stagingDb, models=[af1, af2]) == 1
