from datetime import date

import pandas as pd

from dataactvalidator.scripts import load_sf133
from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory


# These columns are used to group SF133 rows when filling in missing data.
# It's everything but 'line' and 'amount'
FINGERPRINT_COLS = [
    'availability_type_code', 'sub_account_code',
    'allocation_transfer_agency', 'fiscal_year', 'beginning_period_of_availa',
    'ending_period_of_availabil', 'main_account_code', 'agency_identifier',
    'period', 'created_at', 'updated_at', 'tas']


def test_fill_blank_sf133_lines_types():
    """Validate that floats aren't downgraded to ints in the pivot_table
    function (that'd be a regression)."""
    data = pd.DataFrame(
        # We'll only pay attention to two of these fields
        [[1440, 3041046.31] + list('ABCDEFGHIJKL')],
        columns=['line', 'amount'] + FINGERPRINT_COLS
    )
    result = load_sf133.fill_blank_sf133_lines(data)
    assert result['amount'][0] == 3041046.31


def test_fill_blank_sf133_lines():
    """This function should fill in missing data if line numbers (i.e. rows)
    of the input are missing"""
    data = pd.DataFrame(
        # Using the letters of 'FINGERPRINTX' to indicate how to group SF133
        # rows. FINGERPRINT1 has rows for line numbers 1 and 2, while
        # FINGERPRINT2 has rows for line numbers 2 and 3. We want both to have
        # line numbers 1 through 3
        [[1, 1] + list('FINGERPRINT1'),
         [2, 2] + list('FINGERPRINT1'),
         [2, 2] + list('FINGERPRINT2'),
         [3, 3] + list('FINGERPRINT2')],
        columns=['line', 'amount'] + FINGERPRINT_COLS
    )
    result = load_sf133.fill_blank_sf133_lines(data)
    assert list(sorted(result[result['line'] == 1]['amount'])) == [0.0, 1.0]
    assert list(sorted(result[result['line'] == 2]['amount'])) == [2.0, 2.0]
    assert list(sorted(result[result['line'] == 3]['amount'])) == [0.0, 3.0]


def test_update_tas_ids_fiscal_year(database):
    """Fiscal year math should be accurate when checking TAS entries"""
    sess = database.session
    tas = TASFactory(internal_start_date=date(2010, 1, 1),
                     internal_end_date=date(2010, 8, 31))
    copied_fields = {   # translate to a slightly different set of fields
        'allocation_transfer_agency': tas.allocation_transfer_agency,
        'agency_identifier': tas.agency_identifier,
        'beginning_period_of_availa': tas.beginning_period_of_availability,
        'ending_period_of_availabil': tas.ending_period_of_availability,
        'availability_type_code': tas.availability_type_code,
        'main_account_code': tas.main_account_code,
        'sub_account_code': tas.sub_account_code
    }
    sf_133 = SF133Factory(fiscal_year=2011, period=1, **copied_fields)
    sess.add_all([tas, sf_133])
    sess.commit()

    load_sf133.update_tas_id(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.tas_id is None

    tas.internal_end_date = date(2010, 9, 30)
    sess.commit()
    load_sf133.update_tas_id(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.tas_id is None

    tas.internal_end_date = date(2010, 10, 31)
    sess.commit()
    load_sf133.update_tas_id(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.tas_id is tas.tas_id
