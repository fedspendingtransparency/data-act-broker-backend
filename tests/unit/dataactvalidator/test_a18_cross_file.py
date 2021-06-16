from random import randint

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a18_cross_file'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_gross_outlay_amount_by_tas_cpe',
                       'target_value_gross_outlay_amount_by_pro_cpe_sum', 'difference', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_sum_matches(database):
    """ Test the GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the sum of the
        corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the award financial file (B).
        {This value is the sum of all Gross Outlay Amounts reported in file B, to indicate year-to-date activity by
        TAS/Subaccount.}
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(account_num=tas.account_num)
    op2 = ObjectClassProgramActivityFactory(account_num=tas.account_num)
    approp_val = sum(op.gross_outlay_amount_by_pro_cpe for op in (op1, op2))
    approp = AppropriationFactory(account_num=tas.account_num, gross_outlay_amount_by_tas_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 0


def test_sum_does_not_match(database):
    """ Test fail the GrossOutlayAmountByTAS_CPE amount in the appropriation file (A) does not equal the sum of the
        corresponding GrossOutlayAmountByProgramObjectClass_CPE values in the award financial file (B).
        {This value is the sum of all Gross Outlay Amounts reported in file B, to indicate year-to-date activity by
        TAS/Subaccount.}
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(account_num=tas.account_num)
    op2 = ObjectClassProgramActivityFactory(account_num=tas.account_num)
    approp_val = sum(op.gross_outlay_amount_by_pro_cpe for op in (op1, op2))
    approp_val += randint(1, 9999)  # different value now
    approp = AppropriationFactory(account_num=tas.account_num, gross_outlay_amount_by_tas_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 1
