from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a26_appropriations'
_TAS = 'a26_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'contract_authority_amount_cpe'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that ContractAuthorityAmountTotal_CPE is provided if TAS has contract authority value
    provided in GTAS """
    tas = "".join([_TAS, "_success"])

    sf1 = SF133(line=1540, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    sf2 = SF133(line=1640, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    ap = Appropriation(job_id=1, row_number=1, tas=tas, contract_authority_amount_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf1, sf2, ap]) == 0


def test_failure(database):
    """ Tests that ContractAuthorityAmountTotal_CPE is not provided if TAS has contract authority value
    provided in GTAS """
    tas = "".join([_TAS, "_failure"])

    sf1 = SF133(line=1540, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                main_account_code="000", sub_account_code="000")
    sf2 = SF133(line=1640, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                main_account_code="000", sub_account_code="000")

    ap = Appropriation(job_id=1, row_number=1, tas=tas, contract_authority_amount_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf1, sf2, ap]) == 1
