from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b12_award_financial_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'by_direct_reimbursable_fun'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test for USSGL 48XX & 49XX (except 487X & 497X) if any one is provided then
    by_direct_reimbursable_fun is not empty """

    af = AwardFinancialFactory()
    assert number_of_errors(_FILE, database, models=[af]) == 0


def test_failure(database):
    """ Test for USSGL 48XX & 49XX (except 487X & 497X) if any one is provided then
    by_direct_reimbursable_fun is empty """

    af = AwardFinancialFactory(ussgl480100_undelivered_or_fyb=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl480100_undelivered_or_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl483100_undelivered_or_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl488100_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl490100_delivered_orde_fyb=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl490100_delivered_orde_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl493100_delivered_orde_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl498100_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl480200_undelivered_or_fyb=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl480200_undelivered_or_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl483200_undelivered_or_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl488200_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl490200_delivered_orde_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl490800_authority_outl_fyb=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl490800_authority_outl_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(ussgl498200_upward_adjustm_cpe=None, by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1

    af = AwardFinancialFactory(by_direct_reimbursable_fun=None)
    assert number_of_errors(_FILE, database, models=[af]) == 1