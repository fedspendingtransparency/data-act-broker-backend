from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'd13_detached_award_financial_assistance_2'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code", "legal_entity_zip5"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Test LegalEntityZIP5 must be blank for foreign recipients (i.e., when LegalEntityCountryCode is not USA)
        USA doesn't affect success """
    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA", legal_entity_zip5="12345")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA", legal_entity_zip5=None)
    det_award_null = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK", legal_entity_zip5=None)
    det_award_null_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK", legal_entity_zip5='')

    errors = number_of_errors(_FILE, database, models=[det_award, det_award_2, det_award_null, det_award_null_2])
    assert errors == 0


def test_failure(database):
    """ Test failure when LegalEntityZIP5 isn't blank for foreign recipients """

    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="UK", legal_entity_zip5="Test")

    errors = number_of_errors(_FILE, database, models=[det_award])
    assert errors == 1
