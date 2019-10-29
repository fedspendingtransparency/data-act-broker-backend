from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs14_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_zip_last4',
                       'uniqueid_AssistanceTransactionUniqueKey'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIPLast4 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
        USA doesn't affect success
    """
    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA', legal_entity_zip_last4='12345',
                                                        correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UsA',
                                                          legal_entity_zip_last4='12345',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA', legal_entity_zip_last4=None,
                                                          correction_delete_indicatr='c')
    det_award_null = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK',
                                                             legal_entity_zip_last4=None,
                                                             correction_delete_indicatr='C')
    det_award_null_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK',
                                                               legal_entity_zip_last4='',
                                                               correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK', legal_entity_zip_last4='Test',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4, det_award_null,
                                                       det_award_null_2])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIPLast4 isn't blank for foreign recipients """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK', legal_entity_zip_last4='Test',
                                                        correction_delete_indicatr='')

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
