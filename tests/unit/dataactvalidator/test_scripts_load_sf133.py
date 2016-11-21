import pandas as pd

from dataactvalidator.scripts import load_sf133


def test_fill_blank_sf133_lines_types():
    """Validate that floats aren't downgraded to ints in the pivot_table
    function (that'd be a regression)."""
    data = pd.DataFrame(
        # We'll only pay attention to two of these fields
        [[1440, 3041046.31] + list('ABCDEFGHIJKL')],
        columns=[
            'line', 'amount',
            'availability_type_code', 'sub_account_code',
            'allocation_transfer_agency', 'fiscal_year',
            'beginning_period_of_availa', 'ending_period_of_availabil',
            'main_account_code', 'agency_identifier', 'period', 'created_at',
            'updated_at', 'tas'
        ])
    result = load_sf133.fill_blank_sf133_lines(data)
    assert result['amount'][0] == 3041046.31
