from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a7_appropriations'
_TAS = 'a7_appropriations_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'budget_authority_unobligat_fyb', 'expected_value_GTAS SF133 Line 1000',
                       'variance'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that SF 133 amount for line 1000 matches Appropriation budget_authority_unobligat_fyb for the specified
        fiscal year and period
    """
    tas_1 = "".join([_TAS, "_success"])
    tas_2 = "".join([_TAS, "_success_2"])

    sf_1 = SF133(line=1000, tas=tas_1, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    sf_2 = SF133(line=1000, tas=tas_2, period=1, fiscal_year=2016, amount=0, agency_identifier="sys",
                 main_account_code="000", sub_account_code="000")
    ap_1 = Appropriation(job_id=1, row_number=1, tas=tas_1, budget_authority_unobligat_fyb=1)
    ap_2 = Appropriation(job_id=2, row_number=1, tas=tas_2, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap_1, ap_2]) == 0


def test_failure(database):
    """ Tests that SF 133 amount for line 1000 does not match Appropriation budget_authority_unobligat_fyb for the
        specified fiscal year and period
    """
    tas = "".join([_TAS, "_failure"])

    sf = SF133(line=1000, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    ap_1 = Appropriation(job_id=1, row_number=1, tas=tas, budget_authority_unobligat_fyb=0)
    ap_2 = Appropriation(job_id=2, row_number=1, tas=tas, budget_authority_unobligat_fyb=None)

    assert number_of_errors(_FILE, database, models=[sf, ap_1, ap_2]) == 2
