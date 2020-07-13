from dataactcore.models.stagingModels import Appropriation
from dataactcore.models.domainModels import SF133
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a22_appropriations'


def test_column_headers(database):
    expected_subset = {'uniqueid_TAS', 'row_number', 'obligations_incurred_total_cpe',
                       'expected_value_GTAS SF133 Line 2190', 'difference'}
    actual = set(query_columns(_FILE, database))
    assert expected_subset == actual


def test_success(database):
    """ Tests that SF 133 amount sum for line 2190 matches Appropriation obligations_incurred_total_cpe
        for the specified fiscal year and period
    """
    tas = 'tas_one_line'

    sf = SF133(line=2190, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
               main_account_code='000', sub_account_code='000')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, obligations_incurred_total_cpe=1)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 0

    # Test with split SF133 lines
    tas = 'tas_two_lines'

    sf_1 = SF133(line=2190, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
                 main_account_code='000', sub_account_code='000', disaster_emergency_fund_code='n')
    sf_2 = SF133(line=2190, tas=tas, period=1, fiscal_year=2016, amount=4, agency_identifier='sys',
                 main_account_code='000', sub_account_code='000', disaster_emergency_fund_code='o')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, obligations_incurred_total_cpe=5)

    assert number_of_errors(_FILE, database, models=[sf_1, sf_2, ap]) == 0


def test_failure(database):
    """ Tests that SF 133 amount sum for line 2190 does not match Appropriation obligations_incurred_total_cpe
        for the specified fiscal year and period
    """
    tas = 'fail_tas'

    sf = SF133(line=2190, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier='sys',
               main_account_code='000', sub_account_code='000')
    ap = Appropriation(job_id=1, row_number=1, tas=tas, obligations_incurred_total_cpe=0)

    assert number_of_errors(_FILE, database, models=[sf, ap]) == 1
