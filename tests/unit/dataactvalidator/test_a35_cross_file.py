from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a35_cross_file'
_TAS = 'a35_cross_file_tas'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'target_prior_year_adjustment',
                       'source_value_deobligations_recoveries_r_cpe',
                       'target_value_ussgl487100_downward_adjus_cpe_sum',
                       'target_value_ussgl497100_downward_adjus_cpe_sum',
                       'target_value_ussgl487200_downward_adjus_cpe_sum',
                       'target_value_ussgl497200_downward_adjus_cpe_sum', 'difference', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test the DeobligationsRecoveriesRefundsOfPriorYearByTAS_CPE amount in the appropriations account file (A) must
        equal the sum of the corresponding DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE values in
        the object class and program activity file (B) where PYA = "X".
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    ap = AppropriationFactory(account_num=tas.account_num, deobligations_recoveries_r_cpe=8)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        account_num=tas.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        account_num=tas.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='x')
    # Doesn't contribute, different PYA
    op_3 = ObjectClassProgramActivityFactory(
        account_num=tas.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2, op_3]) == 0


def test_success_scenario2(database):
    """ Test the DeobligationsRecoveriesRefundsOfPriorYearByTAS_CPE amount in the appropriations account file (A) must
        equal the sum of the corresponding DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE values in
        the object class and program activity file (B) where PYA = "X".
    """
    tas1 = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas1, tas2])
    database.session.flush()

    ap = AppropriationFactory(account_num=tas1.account_num, deobligations_recoveries_r_cpe=8)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        account_num=tas1.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='x')
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        account_num=tas1.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')
    # Doesn't contribute, different TAS
    op_3 = ObjectClassProgramActivityFactory(
        account_num=tas2.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')
    # Doesn't contribute, different PYA
    op_4 = ObjectClassProgramActivityFactory(
        account_num=tas1.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='A')
    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2, op_3, op_4]) == 0


def test_failure(database):
    """ Test fail the DeobligationsRecoveriesRefundsOfPriorYearByTAS_CPE amount in the appropriations account file (A)
        does not equal the sum of the corresponding DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE
        values in the object class and program activity file (B).
    """
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    ap = AppropriationFactory(account_num=tas.account_num, deobligations_recoveries_r_cpe=7)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        account_num=tas.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='x')
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        account_num=tas.account_num, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1, prior_year_adjustment='X')

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2]) == 1
