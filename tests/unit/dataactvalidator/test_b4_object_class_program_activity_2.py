from tests.unit.dataactcore.factories.staging import ObjectClassProgramActivityFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal


_FILE = "b4_object_class_program_activity_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "prior_year_adjustment",
        "obligations_delivered_orde_cpe",
        "ussgl490100_delivered_orde_cpe",
        "ussgl490110_rein_deliv_ord_cpe",
        "ussgl493100_delivered_orde_cpe",
        "ussgl498100_upward_adjustm_cpe",
        "difference",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_ProgramActivityCode",
        "uniqueid_ProgramActivityName",
        "uniqueid_ObjectClass",
        "uniqueid_ByDirectReimbursableFundingSource",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """Test that calculation passes with equal values and with a null"""

    value1 = Decimal("100.00")
    value2 = Decimal("200.00")
    value3 = Decimal("200.00")
    value4 = Decimal("50.00")
    ocpa = ObjectClassProgramActivityFactory(
        obligations_delivered_orde_cpe=value1 + value2 + value3 + value4,
        ussgl490100_delivered_orde_cpe=value1,
        ussgl493100_delivered_orde_cpe=value2,
        ussgl498100_upward_adjustm_cpe=value3,
        ussgl490110_rein_deliv_ord_cpe=value4,
        prior_year_adjustment="X",
    )
    ocpa_null = ObjectClassProgramActivityFactory(
        obligations_delivered_orde_cpe=value1,
        ussgl490100_delivered_orde_cpe=None,
        ussgl493100_delivered_orde_cpe=None,
        ussgl498100_upward_adjustm_cpe=value1,
        ussgl490110_rein_deliv_ord_cpe=None,
        prior_year_adjustment="X",
    )
    # Different values, different PYA
    ocpa_pya = ObjectClassProgramActivityFactory(
        obligations_delivered_orde_cpe=1,
        ussgl490100_delivered_orde_cpe=0,
        ussgl493100_delivered_orde_cpe=0,
        ussgl498100_upward_adjustm_cpe=0,
        ussgl490110_rein_deliv_ord_cpe=2,
        prior_year_adjustment="A",
    )

    assert number_of_errors(_FILE, database, models=[ocpa, ocpa_null, ocpa_pya]) == 0


def test_failure(database):
    """Test that calculation fails for unequal values"""
    value = Decimal("500.00")
    value2 = Decimal("100.00")
    ocpa = ObjectClassProgramActivityFactory(
        obligations_delivered_orde_cpe=value,
        ussgl490100_delivered_orde_cpe=value2,
        ussgl493100_delivered_orde_cpe=value2,
        ussgl498100_upward_adjustm_cpe=value2,
        ussgl490110_rein_deliv_ord_cpe=value2,
        prior_year_adjustment="X",
    )

    assert number_of_errors(_FILE, database, models=[ocpa]) == 1
