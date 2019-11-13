from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a35_cross_file'
_TAS = 'a35_cross_file_tas'


def test_column_headers(database):
    expected_subset = {'source_row_number', 'source_value_deobligations_recoveries_r_cpe',
                       'target_value_ussgl487100_downward_adjus_cpe_sum',
                       'target_value_ussgl497100_downward_adjus_cpe_sum',
                       'target_value_ussgl487200_downward_adjus_cpe_sum',
                       'target_value_ussgl497200_downward_adjus_cpe_sum', 'difference', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Tests that, for entries with the matching TAS, Appropriations
    deobligations_recoveries_r_cpe equals the sum of all corresponding entries
    for Object Class Program Acitivity fields ussgl487100_downward_adjus_cpe,
    ussgl497100_downward_adjus_cpe, ussgl487200_downward_adjus_cpe,
    ussgl497200_downward_adjus_cpe"""
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    ap = AppropriationFactory(tas_id=tas.tas_id, deobligations_recoveries_r_cpe=8)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        tas_id=tas.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        tas_id=tas.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2]) == 0


def test_success_scenario2(database):
    tas1 = TASFactory()
    tas2 = TASFactory()
    database.session.add_all([tas1, tas2])
    database.session.flush()

    ap = AppropriationFactory(tas_id=tas1.tas_id, deobligations_recoveries_r_cpe=8)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        tas_id=tas1.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        tas_id=tas1.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)
    # Doesn't contribute, different TAS
    op_3 = ObjectClassProgramActivityFactory(
        tas_id=tas2.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)
    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2, op_3]) == 0


def test_failure(database):
    """ Tests that, for entries with the matching TAS, Appropriations
    deobligations_recoveries_r_cpe does not equals the sum of all
    corresponding entries for Object Class Program Acitivity fields
    ussgl487100_downward_adjus_cpe, ussgl497100_downward_adjus_cpe,
    ussgl487200_downward_adjus_cpe, ussgl497200_downward_adjus_cpe"""
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    ap = AppropriationFactory(tas_id=tas.tas_id, deobligations_recoveries_r_cpe=7)
    # Contributes 4
    op_1 = ObjectClassProgramActivityFactory(
        tas_id=tas.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)
    # Contributes another 4
    op_2 = ObjectClassProgramActivityFactory(
        tas_id=tas.tas_id, ussgl487100_downward_adjus_cpe=1, ussgl497100_downward_adjus_cpe=1,
        ussgl487200_downward_adjus_cpe=1, ussgl497200_downward_adjus_cpe=1)

    assert number_of_errors(_FILE, database, models=[ap, op_1, op_2]) == 1
