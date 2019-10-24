from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs44_detached_award_financial_assistance_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'legal_entity_country_code', 'legal_entity_congressional',
                       'uniqueid_afa_generated_unique'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test when LegalEntityCountryCode is not USA, LegalEntityCongressionalDistrict must be blank. This rule is
        ignored when CorrectionDeleteIndicator is D.
    """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA',
                                                          legal_entity_congressional=None,
                                                          correction_delete_indicatr='C')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='usA',
                                                          legal_entity_congressional='',
                                                          correction_delete_indicatr=None)
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='USA',
                                                          legal_entity_congressional='01',
                                                          correction_delete_indicatr='D')
    det_award_4 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='ABc',
                                                          legal_entity_congressional='',
                                                          correction_delete_indicatr='')
    det_award_5 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='',
                                                          legal_entity_congressional=None,
                                                          correction_delete_indicatr='C')
    det_award_6 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='ABC',
                                                          legal_entity_congressional='01',
                                                          correction_delete_indicatr='d')

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2, det_award_3, det_award_4, det_award_5,
                                                       det_award_6])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityCountryCode is not USA, LegalEntityCongressionalDistrict must be blank. """
    det_award_1 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='AbC',
                                                          legal_entity_congressional='02',
                                                          correction_delete_indicatr='C')
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code='',
                                                          legal_entity_congressional='42',
                                                          correction_delete_indicatr=None)

    errors = number_of_errors(_FILE, database, models=[det_award_1, det_award_2])
    assert errors == 2
