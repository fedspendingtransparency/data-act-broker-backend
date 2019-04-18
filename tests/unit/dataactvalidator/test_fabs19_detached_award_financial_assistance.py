from tests.unit.dataactcore.factories.staging import DetachedAwardFinancialAssistanceFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = 'fabs19_detached_award_financial_assistance'


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ LegalEntityCountryCode must contain a valid three character GENC country code. U.S. Territories and Freely
        Associated States must be submitted with country code = USA and their state/territory code; they cannot be
        submitted with their GENC country code. For a list of these territories and more information, see Appendix
        B of the DAIMS Practices and Procedures.
    """
    cc_1 = CountryCode(country_code="USA", country_name="United States", territory_free_state=False)
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine", territory_free_state=False)
    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="USA")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="uKr")

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, det_award, det_award_2])
    assert errors == 0


def test_failure(database):
    """ LegalEntityCountryCode must contain a valid three character GENC country code. U.S. Territories and Freely
        Associated States must be submitted with country code = USA and their state/territory code; they cannot be
        submitted with their GENC country code. For a list of these territories and more information, see Appendix B
        of the DAIMS Practices and Procedures.
    """
    cc_1 = CountryCode(country_code="ASM", country_name="AMERICAN SAMOA", territory_free_state=True)
    det_award = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="xyz")
    det_award_2 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="ABCD")
    det_award_3 = DetachedAwardFinancialAssistanceFactory(legal_entity_country_code="ASM")

    errors = number_of_errors(_FILE, database, models=[cc_1, det_award, det_award_2, det_award_3])
    assert errors == 3
