from tests.unit.dataactcore.factories.domain import SF133Factory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a35_cross_file'
_TAS = 'a35_cross_file_tas'


def test_column_headers(database):
    expected_subset = {'row_number', 'deobligations_recoveries_r_cpe', 'ussgl487100_downward_adjus_cpe_sum',
                       'ussgl497100_downward_adjus_cpe_sum', 'ussgl487200_downward_adjus_cpe_sum',
                       'ussgl497200_downward_adjus_cpe_sum'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that, for entries with the matching TAS, Appropriations deobligations_recoveries_r_cpe equals the sum of
    all corresponding entries for Object Class Program Acitivity fields ussgl487100_downward_adjus_cpe,
    ussgl497100_downward_adjus_cpe, ussgl487200_downward_adjus_cpe, ussgl497200_downward_adjus_cpe"""

    tas_ignore = ''.join([_TAS + "_ignore"])

    ap = AppropriationFactory(tas=_TAS, deobligations_recoveries_r_cpe=8)

    op_1 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    op_2 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2]) == 0

    ap = AppropriationFactory(tas=_TAS, deobligations_recoveries_r_cpe=8)

    op_1 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    op_2 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    op_3 = ObjectClassProgramActivityFactory(tas=tas_ignore, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2, op_3]) == 0


def test_failure(database):
    """ Tests that, for entries with the matching TAS, Appropriations deobligations_recoveries_r_cpe does not equals
    the sum of all corresponding entries for Object Class Program Acitivity fields ussgl487100_downward_adjus_cpe,
    ussgl497100_downward_adjus_cpe, ussgl487200_downward_adjus_cpe, ussgl497200_downward_adjus_cpe"""

    tas_ignore = ''.join([_TAS + "_ignore"])

    ap = AppropriationFactory(tas=_TAS, deobligations_recoveries_r_cpe=7)

    op_1 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1,
                                             ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    op_2 = ObjectClassProgramActivityFactory(tas=_TAS, ussgl487100_downward_adjus_cpe=1,
                                             ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    op_3 = ObjectClassProgramActivityFactory(tas=tas_ignore, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
                                             ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2, op_3]) == 1