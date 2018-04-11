from tests.unit.dataactcore.factories.staging import AppropriationFactory
from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory
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

    ap1 = AppropriationFactory(job_id=1, row_number=1, tas=tas)
    ap2 = AppropriationFactory(job_id=1, row_number=2, tas=tas)

    sf = SF133Factory(line=1021, tas=tas, period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                      main_account_code="000", sub_account_code="000")

    assert number_of_errors(_FILE, database, models=[ap1, ap2, sf]) == 0


def test_failure_with_rule_exception(database):
    """ Tests that TAS for File A are not present in SF-133
    except when all monetary amounts are zero for the TAS"""
    tas = "".join([_TAS, "_failure"])

    ap1 = AppropriationFactory(job_id=1, row_number=1, tas=tas, adjustments_to_unobligated_cpe=1)
    ap2 = AppropriationFactory(job_id=1, row_number=2, tas=tas, budget_authority_appropria_cpe=2)
    ap3 = AppropriationFactory(job_id=1, row_number=3, tas=tas, adjustments_to_unobligated_cpe=0,
                               budget_authority_appropria_cpe=0, borrowing_authority_amount_cpe=0,
                               contract_authority_amount_cpe=0, spending_authority_from_of_cpe=0,
                               other_budgetary_resources_cpe=0, budget_authority_available_cpe=0,
                               gross_outlay_amount_by_tas_cpe=0, obligations_incurred_total_cpe=0,
                               deobligations_recoveries_r_cpe=0, unobligated_balance_cpe=0,
                               status_of_budgetary_resour_cpe=0)

    sf = SF133Factory(line=1021, tas='1', period=1, fiscal_year=2016, amount=1, agency_identifier="sys",
                      main_account_code="000", sub_account_code="000")

    assert number_of_errors(_FILE, database, models=[ap1, ap2, ap3, sf]) == 2


def test_financial_tas_approp(database):
    """ Tests that TAS for File A are not present in SF-133
    except when a financial account (financial indicator type F)"""
    tas_1 = TASFactory(financial_indicator2='other indicator')
    tas_2 = TASFactory(financial_indicator2=None)

    ap_1 = AppropriationFactory(tas_id=tas_1.account_num)
    ap_2 = AppropriationFactory(tas_id=tas_2.account_num)

    assert number_of_errors(_FILE, database, models=[tas_1, tas_2, ap_1, ap_2]) == 2

    tas_3 = TASFactory(financial_indicator2='F')
    tas_4 = TASFactory(financial_indicator2='f')

    ap_3 = AppropriationFactory(tas_id=tas_3.account_num)
    ap_4 = AppropriationFactory(tas_id=tas_4.account_num)

    assert number_of_errors(_FILE, database, models=[tas_3, tas_4, ap_3, ap_4]) == 0
