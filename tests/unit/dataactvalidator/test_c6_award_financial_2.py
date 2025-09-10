from tests.unit.dataactcore.factories.staging import AwardFinancialFactory
from tests.unit.dataactvalidator.utils import number_of_errors, query_columns
from decimal import Decimal

_FILE = "c6_award_financial_2"


def test_column_headers(database):
    expected_subset = {
        "row_number",
        "gross_outlays_undelivered_cpe",
        "ussgl480200_undelivered_or_cpe",
        "ussgl480210_rein_undel_obs_cpe",
        "ussgl483200_undelivered_or_cpe",
        "ussgl488200_upward_adjustm_cpe",
        "difference",
        "uniqueid_TAS",
        "uniqueid_DisasterEmergencyFundCode",
        "uniqueid_PriorYearAdjustment",
        "uniqueid_PIID",
        "uniqueid_FAIN",
        "uniqueid_URI",
    }
    actual = set(query_columns(_FILE, database))
    assert (actual & expected_subset) == expected_subset


def test_success(database):
    """Test that calculation passes with equal values and with a null"""

    value_one = Decimal("101.23")
    value_two = Decimal("102.34")
    value_three = Decimal("103.45")
    value_four = Decimal("104.56")
    award_fin = AwardFinancialFactory(
        gross_outlays_undelivered_cpe=value_one + value_two + value_three + value_four,
        ussgl480200_undelivered_or_cpe=value_one,
        ussgl480210_rein_undel_obs_cpe=value_four,
        ussgl483200_undelivered_or_cpe=value_two,
        ussgl488200_upward_adjustm_cpe=value_three,
    )
    award_fin_null = AwardFinancialFactory(
        gross_outlays_undelivered_cpe=value_one,
        ussgl480200_undelivered_or_cpe=None,
        ussgl480210_rein_undel_obs_cpe=None,
        ussgl483200_undelivered_or_cpe=None,
        ussgl488200_upward_adjustm_cpe=value_one,
    )

    assert number_of_errors(_FILE, database, models=[award_fin, award_fin_null]) == 0


def test_failure(database):
    """Test that calculation fails for unequal values"""
    value_one = Decimal("101.23")
    value_two = Decimal("102.34")
    award_fin = AwardFinancialFactory(
        gross_outlays_undelivered_cpe=value_one,
        ussgl480200_undelivered_or_cpe=value_two,
        ussgl480210_rein_undel_obs_cpe=value_two,
        ussgl483200_undelivered_or_cpe=value_two,
        ussgl488200_upward_adjustm_cpe=value_two,
    )

    assert number_of_errors(_FILE, database, models=[award_fin]) == 1
