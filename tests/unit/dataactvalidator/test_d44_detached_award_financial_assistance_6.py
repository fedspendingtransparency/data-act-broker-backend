from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd44_detached_award_financial_assistance_6'


def test_column_headers(database):
    expected_subset = {"row_number", "awardee_or_recipient_uniqu", "record_type", "business_types"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_pubished_date_success(database):
    """ The DUNS must be blank for aggregate records (i.e., when RecordType = 1) and individual recipients
-- (i.e., when BusinessTypes includes "P"). """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="ABCDEFGHI", record_type=1,
                                                          business_types="AAP")
    # Different record type
    det_award_2 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="", record_type=2,
                                                          business_types="AAP")
    # Different business types
    det_award_3 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu=None, record_type=1,
                                                          business_types="AAA")
    # Handled by d18
    det_award_4 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="123456789", record_type=1,
                                                          business_types="0000PPPP")
    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_pubished_date_failure(database):
    """ Test failure The DUNS must be blank for aggregate records (i.e., when RecordType = 1)
        and individual recipients (i.e., when BusinessTypes includes "P")."""
    det_award_1 = DetachedAwardFinancialAssistanceFactory(awardee_or_recipient_uniqu="", record_type=1,
                                                          business_types="AAP")
    errors = number_of_errors(_FILE, database, models=[det_award_1])
    assert errors == 1
