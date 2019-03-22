from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import (DetachedAwardFinancialAssistanceFactory,
                                                      PublishedAwardFinancialAssistanceFactory)
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_detached_award_financial_assistance_4_2'


def test_column_headers(database):
    expected_subset = {"row_number", "awarding_office_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success_ignore_null_pafa(database):
    """ Test that empty awarding office codes aren't matching invalid office codes from the base record. """

    office = OfficeFactory(office_code='12345a', grant_office=True)
    # Base record has no awarding office code, future records don't affect it
    pub_award_1 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='zyxwv_123',
                                                           action_date='20181018', award_modification_amendme='0',
                                                           is_active=True)
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abc', unique_award_key='zyxwv_123',
                                                           action_date='20181019', award_modification_amendme='1',
                                                           is_active=True)
    # Base record has an invalid code but new record has a awarding office entered (ignore this rule)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abc', unique_award_key='abcd_123',
                                                           action_date='20181019', award_modification_amendme='0',
                                                           is_active=True)
    # Base record with a valid office code (case insensitive)
    pub_award_4 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='12345A', unique_award_key='1234_abc',
                                                           action_date='20181019', award_modification_amendme='0',
                                                           is_active=True)
    # Earliest record inactive, newer record has valid entry
    pub_award_5 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abc', unique_award_key='4321_cba',
                                                           action_date='20181018', award_modification_amendme='0',
                                                           is_active=False)
    pub_award_6 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='12345a', unique_award_key='4321_cba',
                                                           action_date='20181019', award_modification_amendme='1',
                                                           is_active=True)

    # New entry for base award with no office code
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='zyxwv_123',
                                                          action_date='20181020', award_modification_amendme='2',
                                                          correction_delete_indicatr=None)
    # New entry for base award with invalid code but entry has a awarding office code
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='abd', unique_award_key='abcd_123',
                                                          action_date='20181020', award_modification_amendme='1',
                                                          correction_delete_indicatr=None)
    # New entry for valid awarding office
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awarding_office_code=None, unique_award_key='1234_abc',
                                                          action_date='20181020', award_modification_amendme='1',
                                                          correction_delete_indicatr=None)
    # Correction to base record (ignore)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='abcd_123',
                                                          action_date='20181019', award_modification_amendme='0',
                                                          correction_delete_indicatr='C')
    # New entry for earliest inactive
    det_award_5 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='4321_cba',
                                                          action_date='20181020', award_modification_amendme='2',
                                                          correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[office, pub_award_1, pub_award_2, pub_award_3, pub_award_4,
                                                       pub_award_5, pub_award_6, det_award_1, det_award_2, det_award_3,
                                                       det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test fail that empty awarding office codes aren't matching invalid office codes from the base record. """

    office_1 = OfficeFactory(office_code='12345a', grant_office=True)
    office_2 = OfficeFactory(office_code='abcd', grant_office=False)
    # Invalid code in record
    pub_award_1 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abc', unique_award_key='zyxwv_123',
                                                           action_date='20181018', award_modification_amendme='0',
                                                           is_active=True)
    # Earliest record inactive, newer record has invalid entry
    pub_award_2 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='12345a', unique_award_key='4321_cba',
                                                           action_date='20181018', award_modification_amendme='0',
                                                           is_active=False)
    pub_award_3 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abc', unique_award_key='4321_cba',
                                                           action_date='20181019', award_modification_amendme='1',
                                                           is_active=True)
    # Has a valid code but it's not an awarding code
    pub_award_4 = PublishedAwardFinancialAssistanceFactory(awarding_office_code='abcd', unique_award_key='123_abc',
                                                           action_date='20181018', award_modification_amendme='0',
                                                           is_active=True)

    # Entry for invalid code in base record
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='zyxwv_123',
                                                          action_date='20181020', award_modification_amendme='2',
                                                          correction_delete_indicatr=None)
    # New entry for earliest inactive
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='4321_cba',
                                                          action_date='20181020', award_modification_amendme='2',
                                                          correction_delete_indicatr=None)
    # New entry for has valid non-awarding code
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awarding_office_code='', unique_award_key='123_abc',
                                                          action_date='20181020', award_modification_amendme='2',
                                                          correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[office_1, office_2, pub_award_1, pub_award_2, pub_award_3,
                                                       pub_award_4, det_award_1, det_award_2, det_award_3])
    assert errors == 3
