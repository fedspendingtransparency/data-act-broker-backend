from random import randint

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a19_cross_file'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'target_prior_year_adjustment',
                       'source_value_obligations_incurred_total_cpe',
                       'target_value_obligations_incurred_by_pr_cpe_sum', 'difference', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_sum_matches(database):
    """ Test the ObligationsIncurredTotalByTAS_CPE amount in the appropriations account file (A) must equal the negative
        (additive inverse) of the sum of the corresponding ObligationsIncurredByProgramObjectClass_CPE values in the
        object class and program activity file (B) where PYA = "X".
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(account_num=tas.account_num, prior_year_adjustment='X')
    op2 = ObjectClassProgramActivityFactory(account_num=tas.account_num, prior_year_adjustment='x')
    op3 = ObjectClassProgramActivityFactory(account_num=tas.account_num, prior_year_adjustment='A')
    approp_val = -sum(op.obligations_incurred_by_pr_cpe for op in (op1, op2))
    approp = AppropriationFactory(account_num=tas.account_num, obligations_incurred_total_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2, op3]) == 0


def test_sum_does_not_match(database):
    """ Test fail the ObligationsIncurredTotalByTAS_CPE amount in the appropriations account file (A) must equal the
        negative (additive inverse) of the sum of the corresponding ObligationsIncurredByProgramObjectClass_CPE values
        in the object class and program activity file (B) where PYA = "X".
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(account_num=tas.account_num, prior_year_adjustment='X')
    op2 = ObjectClassProgramActivityFactory(account_num=tas.account_num, prior_year_adjustment='x')
    approp_val = -sum(op.obligations_incurred_by_pr_cpe for op in (op1, op2))
    approp_val += randint(1, 9999)  # different value now
    approp = AppropriationFactory(account_num=tas.account_num, obligations_incurred_total_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 1
