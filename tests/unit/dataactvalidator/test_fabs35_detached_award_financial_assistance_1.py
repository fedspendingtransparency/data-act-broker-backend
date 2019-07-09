from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs35_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_zip_last4'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ When provided, LegalEntityZIPLast4 must be in the format ####. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='1234', correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4=None, correction_delete_indicatr='c')
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='', correction_delete_indicatr=None)
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='12345',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ When provided, LegalEntityZIPLast4 must be in the format ####. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='123',
                                                          correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='12345',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='ABCD',
                                                          correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_zip_last4='123D',
                                                          correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4])
    assert errors == 4
