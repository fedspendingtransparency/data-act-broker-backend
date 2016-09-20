from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors


_FILE = 'a11_appropriations'
_TAS = 'a11_appropriations_tas'


def test_success(database):
    """ Tests that SF 133 amount sum for lines 1750, 1850 matches Appropriation spending_authority_from_of_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_success"])

    sf_1 = SF133(line=1750, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    sf_2 = SF133(line=1850, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, spending_authority_from_of_cpe=2)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database, models=models) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for lines 1750, 1850 does not match Appropriation spending_authority_from_of_cpe
        for the specified fiscal year and period """
    tas = "".join([_TAS, "_failure"])

    sf_1 = SF133(line=1750, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    sf_2 = SF133(line=1850, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
               main_account_code="000", sub_account_code="000")
    ap = Appropriation(job_id=1, row_number=1, tas=tas, spending_authority_from_of_cpe=1)

    models = [sf_1, sf_2, ap]

    assert number_of_errors(_FILE, database, models=models) == 1
