from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs18_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'business_types', 'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X.
    """
    det_award = DetachedAwardFinancialAssistanceFactory(business_types='A', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types='XB', correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_types='RCm', correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(business_types='rcm', correction_delete_indicatr='C')
    # Ignore correction delete indicator of D
    det_award_5 = DetachedAwardFinancialAssistanceFactory(business_types='BOB', correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ BusinessTypes must be one to three letters in length. BusinessTypes values must be non-repeated letters
        from A to X.
    """

    # Test if it's somehow empty or has 4 letters (length test)
    det_award = DetachedAwardFinancialAssistanceFactory(business_types='', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types='ABCD', correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2

    # Test repeats
    det_award = DetachedAwardFinancialAssistanceFactory(business_types='BOb', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types='BOB', correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_types='BbO', correction_delete_indicatr='')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(business_types='BB', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 4

    # Test that only valid letters work
    det_award = DetachedAwardFinancialAssistanceFactory(business_types='ABY', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(business_types='C2', correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(business_types='c2d', correction_delete_indicatr='')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(business_types='123', correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 4
