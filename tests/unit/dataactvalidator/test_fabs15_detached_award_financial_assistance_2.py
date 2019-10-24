from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs15_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_foreign_city', 'record_type',
                       'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success LegalEntityForeignCityName must be blank for domestic recipients (LegalEntityCountryCode = USA) and
        for aggregate records (RecordType = 1).
    """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UsA',
                                                        legal_entity_foreign_city=None,
                                                        record_type=2, correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA',
                                                          legal_entity_foreign_city='',
                                                          record_type=3, correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UKR',
                                                          legal_entity_foreign_city='',
                                                          record_type=1, correction_delete_indicatr='c')
    # Ignore correction delete indicator of D
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UKR',
                                                          legal_entity_foreign_city='Test City',
                                                          record_type=1, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_3, det_award_4])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityForeignCityName must be blank for domestic recipients (LegalEntityCountryCode = USA) and
        for aggregate records (RecordType = 1).
    """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UsA',
                                                        legal_entity_foreign_city='New York',
                                                        record_type=2, correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UKR',
                                                          legal_entity_foreign_city='Test City',
                                                          record_type=1, correction_delete_indicatr='C')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
