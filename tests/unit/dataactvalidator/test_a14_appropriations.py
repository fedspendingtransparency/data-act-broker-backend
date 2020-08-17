from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a14_appropriations'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'gross_outlay_amount_by_tas_cpe',
                       'expected_value_GTAS SF133 Line 3020', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that SF 133 amount sum for line 3020 matches Appropriation gross_outlay_amount_by_tas_cpe
        for the specified fiscal year and period
    """
    tas = 'tas_one_line'

    sf = SF133(line=3020, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
               main_account_code='000', sub_account_code='000')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, display_tas=tas, gross_outlay_amount_by_tas_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0

    # Test with split SF133 lines
    tas = 'tas_two_lines'

    sf_1 = SF133(line=3020, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
                 main_account_code='000', sub_account_code='000', disaster_emergency_fund_code='n')
    sf_2 = SF133(line=3020, tas=tas, period=1, fiscal_year=2016, amount=4, agency_identifier='sys',
                 main_account_code='000', sub_account_code='000', disaster_emergency_fund_code='o')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, gross_outlay_amount_by_tas_cpe=5)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for line 3020 does not match Appropriation gross_outlay_amount_by_tas_cpe
        for the specified fiscal year and period
    """
    tas = 'fail_tas'

    sf = SF133(line=3020, tas=tas, display_tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
               main_account_code='000', sub_account_code='000')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, display_tas=tas, gross_outlay_amount_by_tas_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1
