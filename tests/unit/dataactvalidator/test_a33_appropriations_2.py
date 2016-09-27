from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a33_appropriations_2'
_TAS = 'a33_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'allocation_transfer_agency', 'agency_identifier',
                       'beginning_period_of_availa', 'ending_period_of_availabil',
                       'availability_type_code', 'main_account_code', 'sub_account_code'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that TAS for File A are present in SF-133 """
    tas = "".join([_TAS, "_success"])

    ap1 = Appropriation(job_id=1, row_number=1, tas=tas)
    ap2 = Appropriation(job_id=1, row_number=2, tas=tas)

    sf = SF133(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    assert number_of_errors(_FILE, database, models=[ap1, ap2, sf]) == 0


def test_failure(database):
    """ Tests that TAS for File A are not present in SF-133 """
    tas = "".join([_TAS, "_failure"])

    ap1 = Appropriation(job_id=1, row_number=1, tas=tas)
    ap2 = Appropriation(job_id=1, row_number=2, tas=tas)

    sf = SF133(line=1021, tas='1', period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")

    assert number_of_errors(_FILE, database, models=[ap1, ap2, sf]) == 2