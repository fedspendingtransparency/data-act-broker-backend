from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a33_appropriations_1'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil',
                       'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database.stagingDb))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS for SF-133 are present in File A """
    tas = "".join([_TAS, "_success"])

    sf1 = SF133(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    sf2 = SF133(line=1033, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")

    ap = Appropriation(job_id=1, row_number=1, tas=tas)

    assert number_of_errors(_FILE, database.stagingDb, models=[sf1, sf2, ap]) == 0


def test_failure(database):
    """ Tests that TAS for SF-133 are not present in File A """
    tas = "".join([_TAS, "_failure"])

    sf1 = SF133(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    sf2 = SF133(line=1033, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    ap = Appropriation(job_id=1, row_number=1, tas='1')

    assert number_of_errors(_FILE, database.stagingDb, models=[sf1, sf2, ap]) == 2
