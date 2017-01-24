from random import randint

from tests.unit.dataactcore.factories.domain import TASFactory
from tests.unit.dataactcore.factories.staging import AppropriationFactory, ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'a19_cross_file'


def test_column_headers(database):
    expected_subset = {
        'row_number', 'allocation_transfer_agency', 'agency_identifier', 'beginning_period_of_availa',
        'ending_period_of_availabil', 'availability_type_code', 'main_account_code', 'sub_account_code',
        'obligations_incurred_total_cpe', 'obligations_incurred_by_pr_cpe_sum'
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_sum_matches(database):
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    op2 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    approp_val = -sum(op.obligations_incurred_by_pr_cpe for op in (op1, op2))
    approp = AppropriationFactory(tas_id=tas.tas_id, obligations_incurred_total_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 0


def test_sum_does_not_match(database):
    tas = TASFactory()
    database.session.add(tas)
    database.session.flush()

    op1 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    op2 = ObjectClassProgramActivityFactory(tas_id=tas.tas_id)
    approp_val = -sum(op.obligations_incurred_by_pr_cpe for op in (op1, op2))
    approp_val += randint(1, 9999)  # different value now
    approp = AppropriationFactory(tas_id=tas.tas_id, obligations_incurred_total_cpe=approp_val)
    assert number_of_errors(_FILE, database, models=[approp, op1, op2]) == 1
