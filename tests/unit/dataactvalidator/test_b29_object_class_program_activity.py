from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns


_FILE = 'b29_object_class_program_activity'


def test_column_headers(database):
    expected_subset = {'row_number', 'prior_year_adjustment', 'uniqueid_TAS'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ PYA must be X, B, or P """

    afs = [
        ObjectClassProgramActivityFactory(prior_year_adjustment='X'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='x'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='B'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='b'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='P'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='p'),
    ]
    assert number_of_errors(_FILE, database, models=afs) == 0


def test_failure(database):
    """ Tests failure if PYA is not X, B, or P"""

    afs = [
        ObjectClassProgramActivityFactory(prior_year_adjustment=''),
        ObjectClassProgramActivityFactory(prior_year_adjustment='Fail'),
        ObjectClassProgramActivityFactory(prior_year_adjustment=0),
        ObjectClassProgramActivityFactory(prior_year_adjustment='None'),
        ObjectClassProgramActivityFactory(prior_year_adjustment='A'),
    ]
    assert number_of_errors(_FILE, database, models=afs) == 5
