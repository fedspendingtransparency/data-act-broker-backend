from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
import pytest

_FILE = 'b12_object_class_program_activity_2'


def test_column_headers(database):
    expected_subset = {'row_number', 'by_direct_reimbursable_fun'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


@pytest.mark.parametrize('op', [ObjectClassProgramActivityFactory(by_direct_reimbursable_fun=None),
                                ObjectClassProgramActivityFactory(by_direct_reimbursable_fun=''),
                                ObjectClassProgramActivityFactory(by_direct_reimbursable_fun='R'),
                                ObjectClassProgramActivityFactory(by_direct_reimbursable_fun='r'),
                                ObjectClassProgramActivityFactory(by_direct_reimbursable_fun='D'),
                                ObjectClassProgramActivityFactory(by_direct_reimbursable_fun='d')])
def test_success(database, op):
    """ Test by_direct_reimbursable_fun is '', R, or D """

    assert number_of_errors(_FILE, database, models=[op]) == 0


def test_failure(database):
    """ Test by_direct_reimbursable_fun is not '', R, or D """

    op = ObjectClassProgramActivityFactory(by_direct_reimbursable_fun='x')
    assert number_of_errors(_FILE, database, models=[op]) == 1