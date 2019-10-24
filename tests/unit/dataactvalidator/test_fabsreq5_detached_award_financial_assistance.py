from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabsreq5_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'correction_delete_indicatr',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityCountryCode is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='C', legal_entity_country_code='USA')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='',
                                                          legal_entity_country_code='GBR')
    # Test ignoring for D records
    det_award_3 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='d',
                                                          legal_entity_country_code=None)
    det_award_4 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', legal_entity_country_code='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='D', legal_entity_country_code='U')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_5])
    assert errors == 0


def test_failure(database):
    """ Test fail LegalEntityCountryCode is required for all submissions except delete records. """

    det_award = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr='c', legal_entity_country_code=None)
    det_award_2 = DetachedAwardFinancialAssistanceFactory(correction_delete_indicatr=None, legal_entity_country_code='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
