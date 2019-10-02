from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a29_appropriations'
_TAS = 'a29_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'deobligations_recoveries_r_cpe',
                       'expected_value_SUM of GTAS SF133 Lines 1021, 1033'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that SF 133 amount sum for lines 1021, 1033 matches Appropriation deobligations_recoveries_r_cpe
        for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_success"])

    sf_1 = SF133(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    sf_2 = SF133(line=1033, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, deobligations_recoveries_r_cpe=2)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database, models=models) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for lines 1021, 1033 does not match Appropriation deobligations_recoveries_r_cpe
        for the specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    sf_2 = SF133(line=1033, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, deobligations_recoveries_r_cpe=1)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database, models=models) == 1
