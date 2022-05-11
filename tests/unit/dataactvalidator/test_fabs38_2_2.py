from tests.unit.dataactcore.factories.domain import OfficeFactory
from tests.unit.dataactcore.factories.staging import FABSFactory, PublishedFABSFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs38_detached_award_financial_assistance_2_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'funding_office_code', 'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success_ignore_null_pub_fabs(database):
    """ Test that empty funding office codes aren't matching invalid office codes from the base record. """

    office_1 = OfficeFactory(office_code='12345b', contract_funding_office=False,
                             financial_assistance_funding_office=True)
    # Base record has no funding office code, future records don't affect it
    pub_fabs_1 = PublishedFABSFactory(funding_office_code='', unique_award_key='zyxwv_123', action_date='20181018',
                                      award_modification_amendme='0', is_active=True)
    pub_fabs_2 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='zyxwv_123', action_date='20181019',
                                      award_modification_amendme='1', is_active=True)
    # Base record has an invalid code but new record has a funding office entered (ignore this rule)
    pub_fabs_3 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='abcd_123', action_date='20181019',
                                      award_modification_amendme='0', is_active=True)
    # Base record with a valid office code (case insensitive)
    pub_fabs_4 = PublishedFABSFactory(funding_office_code='12345B', unique_award_key='1234_abc', action_date='20181019',
                                      award_modification_amendme='0', is_active=True)
    # Earliest record inactive, newer record has valid entry, inactive date matching active doesn't mess it up
    pub_fabs_5 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='4321_cba', action_date='20181018',
                                      award_modification_amendme='0', is_active=False)
    pub_fabs_6 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='4321_cba', action_date='20181019',
                                      award_modification_amendme='1', is_active=False)
    pub_fabs_7 = PublishedFABSFactory(funding_office_code='12345b', unique_award_key='4321_cba', action_date='20181019',
                                      award_modification_amendme='1', is_active=True)

    # New entry for base award with no office code
    fabs_1 = FABSFactory(funding_office_code='', unique_award_key='zyxwv_123', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    # New entry for base award with invalid code but entry has a funding office code
    fabs_2 = FABSFactory(funding_office_code='abd', unique_award_key='abcd_123', action_date='20181020',
                         award_modification_amendme='1', correction_delete_indicatr=None)
    # New entry for valid funding office
    fabs_3 = FABSFactory(funding_office_code=None, unique_award_key='1234_abc', action_date='20181020',
                         award_modification_amendme='1', correction_delete_indicatr=None)
    # Correction to base record (ignore)
    fabs_4 = FABSFactory(funding_office_code='', unique_award_key='abcd_123', action_date='20181019',
                         award_modification_amendme='0', correction_delete_indicatr='C')
    # New entry for earliest inactive
    fabs_5 = FABSFactory(funding_office_code='', unique_award_key='4321_cba', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[office_1, pub_fabs_1, pub_fabs_2, pub_fabs_3, pub_fabs_4,
                                                       pub_fabs_5, pub_fabs_6, pub_fabs_7, fabs_1, fabs_2, fabs_3,
                                                       fabs_4, fabs_5])
    assert errors == 0


def test_failure(database):
    """ Test fail that empty funding office codes aren't matching invalid office codes from the base record. """

    office_1 = OfficeFactory(office_code='12345a', contract_funding_office=False,
                             financial_assistance_funding_office=True)
    office_2 = OfficeFactory(office_code='abcd', contract_funding_office=True,
                             financial_assistance_funding_office=False)
    # Invalid code in record
    pub_fabs_1 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='zyxwv_123', action_date='20181018',
                                      award_modification_amendme='0', is_active=True)
    # Earliest record inactive, newer record has invalid entry
    pub_fabs_2 = PublishedFABSFactory(funding_office_code='12345a', unique_award_key='4321_cba', action_date='20181018',
                                      award_modification_amendme='0', is_active=False)
    pub_fabs_3 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='4321_cba', action_date='20181019',
                                      award_modification_amendme='1', is_active=True)
    # Has a valid code but it's not a funding assistance office
    pub_fabs_4 = PublishedFABSFactory(funding_office_code='abcd', unique_award_key='123_abc', action_date='20181018',
                                      award_modification_amendme='0', is_active=True)
    # award_modification_amendme number is null
    pub_fabs_5 = PublishedFABSFactory(funding_office_code='abc', unique_award_key='zyxwv_1234', action_date='20181018',
                                      award_modification_amendme=None, is_active=True)

    # Entry for invalid code in base record
    fabs_1 = FABSFactory(funding_office_code='', unique_award_key='zyxwv_123', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    # Entry with award_modification_amendme null
    fabs_2 = FABSFactory(funding_office_code='', unique_award_key='zyxwv_123', action_date='20181020',
                         award_modification_amendme=None, correction_delete_indicatr=None)
    # New entry for earliest inactive
    fabs_3 = FABSFactory(funding_office_code='', unique_award_key='4321_cba', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    # New entry for has valid non-funding assistance code
    fabs_4 = FABSFactory(funding_office_code='', unique_award_key='123_abc', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    # Entry for award_modification_amendme null in base record
    fabs_5 = FABSFactory(funding_office_code='', unique_award_key='zyxwv_1234', action_date='20181020',
                         award_modification_amendme='2', correction_delete_indicatr=None)
    errors = number_of_errors(_FILE, database, models=[office_1, office_2, pub_fabs_1, pub_fabs_2, pub_fabs_3,
                                                       pub_fabs_4, pub_fabs_5, fabs_1, fabs_2, fabs_3, fabs_4, fabs_5])
    assert errors == 5
