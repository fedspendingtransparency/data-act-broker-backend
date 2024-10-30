from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal


_FILE = 'b4_object_class_program_activity_1'


def test_column_headers(database):
    expected_subset = {'row_number', 'prior_year_adjustment', 'obligations_delivered_orde_fyb',
                       'ussgl490100_delivered_orde_fyb', 'difference', 'uniqueid_TAS',
                       'uniqueid_DisasterEmergencyFundCode', 'uniqueid_ProgramActivityCode',
                       'uniqueid_ProgramActivityName', 'uniqueid_ObjectClass',
                       'uniqueid_ByDirectReimbursableFundingSource'}
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """ Test that calculation passes with equal values and with a null """

    value = Decimal('101.23')
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb=value,
                                             ussgl490100_delivered_orde_fyb=value,
                                             prior_year_adjustment='X')
    ocpa_null = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb=0,
                                                  ussgl490100_delivered_orde_fyb=None,
                                                  prior_year_adjustment='x')
    # Different values, different PYA
    ocpa_pya = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb=0,
                                                 ussgl490100_delivered_orde_fyb=1,
                                                 prior_year_adjustment='A')

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null, ocpa_pya]) == 0


def test_failure(database):
    """ Test that calculation fails for unequal values """
    ocpa = ObjectClassProgramActivityFactory(obligations_delivered_orde_fyb=Decimal('101.23'),
                                             ussgl490100_delivered_orde_fyb=Decimal('102.34'),
                                             prior_year_adjustment='x')

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
