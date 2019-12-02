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
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    op2 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    approp_val = sum(op.gross_outlay_amount_by_pro_cpe for op in (op1, op2))
    approp = AppropriationFactory(tas_id=tas.tas_id, gross_outlay_amount_by_tas_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 0


def test_sum_does_not_match(database):
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    op2 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    approp_val = sum(op.gross_outlay_amount_by_pro_cpe for op in (op1, op2))
    approp_val += randint(1, 9999)  # different value now
    approp = AppropriationFactory(tas_id=tas.tas_id, gross_outlay_amount_by_tas_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 1
