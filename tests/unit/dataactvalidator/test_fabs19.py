from tests.unit.dataactcore.factories.staging import FABSFactory
from dataactcore.models.domainModels import CountryCode
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns

_FILE = "fabs19"


def test_column_headers(database):
    expected_subset = {"row_number", "legal_entity_country_code", "uniqueid_AssistanceTransactionUniqueKey"}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """LegalEntityCountryCode must contain a valid three character GENC country code. U.S. Territories and Freely
    Associated States must be submitted with country code = USA and their state/territory code; they cannot be
    submitted with their GENC country code.
    """
    cc_1 = CountryCode(country_code="USA", country_name="United States", territory_free_state=False)
    cc_2 = CountryCode(country_code="UKR", country_name="Ukraine", territory_free_state=False)
    fabs = FABSFactory(legal_entity_country_code="USA", correction_delete_indicatr="")
    fabs_2 = FABSFactory(legal_entity_country_code="uKr", correction_delete_indicatr="c")
    # Ignore correction delete indicator of D
    fabs_3 = FABSFactory(legal_entity_country_code="ABCD", correction_delete_indicatr="d")

    errors = number_of_errors(_FILE, database, models=[cc_1, cc_2, fabs, fabs_2, fabs_3])
    assert errors == 0


def test_failure(database):
    """LegalEntityCountryCode must contain a valid three character GENC country code. U.S. Territories and Freely
    Associated States must be submitted with country code = USA and their state/territory code; they cannot be
    submitted with their GENC country code.
    """
    cc_1 = CountryCode(country_code="ASM", country_name="AMERICAN SAMOA", territory_free_state=True)
    fabs = FABSFactory(legal_entity_country_code="xyz", correction_delete_indicatr="c")
    fabs_2 = FABSFactory(legal_entity_country_code="ABCD", correction_delete_indicatr="")
    fabs_3 = FABSFactory(legal_entity_country_code="ASM", correction_delete_indicatr="")

    errors = number_of_errors(_FILE, database, models=[cc_1, fabs, fabs_2, fabs_3])
    assert errors == 3
