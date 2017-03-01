from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd18_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "business_types"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X. """
    det_award = DetachedAwardFinancialAssistanceFactory(business_types="A")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types="XB")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_types="RCm")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3])
    assert errors == 0


def test_failure(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X. """

    # Test if it's somehow empty or has 4 letters (length test)
    det_award = DetachedAwardFinancialAssistanceFactory(business_types="")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types="ABCD")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2

    # Test repeats
    det_award = DetachedAwardFinancialAssistanceFactory(business_types="BOb")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types="BOB")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_types="BbO")
    det_award_4 = DetachedAwardFinancialAssistanceFactory(business_types="BB")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 4

    # Test that only valid letters work
    det_award = DetachedAwardFinancialAssistanceFactory(business_types="ABY")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types="C2")

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
