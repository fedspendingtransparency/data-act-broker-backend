from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs15_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_foreign_city', 'record_type'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test success LegalEntityForeignCityName is required for foreign recipients (i.e., when
        LegalEntityCountryCode != USA) for non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3)
    """

    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='Japan',
                                                          legal_entity_foreign_city='Tokyo',
                                                          record_type=2, correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK',
                                                          legal_entity_foreign_city='Manchester',
                                                          record_type=3, correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA',
                                                          legal_entity_foreign_city=None,
                                                          record_type=2, correction_delete_indicatr='c')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UsA',
                                                          legal_entity_foreign_city='',
                                                          record_type=3, correction_delete_indicatr='C')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='UK',
                                                          legal_entity_foreign_city='',
                                                          record_type=1, correction_delete_indicatr='')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='CAN',
                                                          legal_entity_foreign_city=None,
                                                          record_type=1, correction_delete_indicatr='')
    # Ignore correction delete indicator of D
    det_award_7 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='Canada',
                                                          legal_entity_foreign_city='',
                                                          record_type=3, correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6, det_award_7])
    assert errors == 0


def test_failure(database):
    """ Test failure LegalEntityForeignCityName is required for foreign recipients (i.e., when
        LegalEntityCountryCode != USA) for non-aggregate and PII-redacted non-aggregate records (RecordType = 2 or 3)
    """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='Japan',
                                                        legal_entity_foreign_city=None,
                                                        record_type=2, correction_delete_indicatr='')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='Canada',
                                                          legal_entity_foreign_city='',
                                                          record_type=3, correction_delete_indicatr='c')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2])
    assert errors == 2
