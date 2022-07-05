import os
from datetime import date
from decimal import Decimal

import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SF133
from dataactvalidator.scripts import load_sf133
from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory


# These columns are used to group SF133 rows when filling in missing data.
# It's everything but 'line' and 'amount'
FINGERPRINT_COLS = [
    'availability_type_code', 'sub_account_code', 'allocation_transfer_agency', 'fiscal_year',
    'beginning_period_of_availa', 'ending_period_of_availabil', 'main_account_code', 'agency_identifier',
    'period', 'created_at', 'updated_at', 'tas', 'display_tas', 'disaster_emergency_fund_code']


def test_fill_blank_sf133_lines_types():
    """ Validate that floats aren't downgraded to ints in the pivot_table function (that'd be a regression)."""
    data = pd.DataFrame(
        # We'll only pay attention to two of these fields
        [[1440, 3041046.31] + list('ABCDEFGHIJKLLQ')], columns=['line', 'amount'] + FINGERPRINT_COLS
    )
    result = load_sf133.fill_blank_sf133_lines(data)
    assert result['amount'][0] == 3041046.31


def test_fill_blank_sf133_lines():
    """ This function should fill in missing data if line numbers (i.e. rows) of the input are missing """
    data = pd.DataFrame(
        # Using the letters of 'FINGERPRINTXX' to indicate how to group SF133 rows.
        # FINGERPRINT1 has rows for line numbers 1 and 2, while FINGERPRINT2 has rows for line numbers 2 and 3.
        # We want both to have line numbers 1 through 3
        [[1, 1] + list('FINGERPRINT11Q'),
         [2, 2] + list('FINGERPRINT11Q'),
         [2, 2] + list('FINGERPRINT22Q'),
         [3, 3] + list('FINGERPRINT22Q')],
        columns=['line', 'amount'] + FINGERPRINT_COLS
    )
    result = load_sf133.fill_blank_sf133_lines(data)
    assert list(sorted(result[result['line'] == 1]['amount'])) == [0.0, 1.0]
    assert list(sorted(result[result['line'] == 2]['amount'])) == [2.0, 2.0]
    assert list(sorted(result[result['line'] == 3]['amount'])) == [0.0, 3.0]


def test_update_account_nums_fiscal_year(database):
    """ Fiscal year math should be accurate when checking TAS entries """
    sess = database.session
    tas = TASFactory(internal_start_date=date(2010, 1, 1), internal_end_date=date(2010, 8, 31))
    sf_133 = SF133Factory(fiscal_year=2011, period=1, **tas.component_dict())
    sess.add_all([tas, sf_133])
    sess.commit()

    load_sf133.update_account_num(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.account_num is None

    tas.internal_end_date = date(2010, 9, 30)
    sess.commit()
    load_sf133.update_account_num(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.account_num is None

    tas.internal_end_date = date(2010, 10, 31)
    sess.commit()
    load_sf133.update_account_num(2011, 1)
    sess.refresh(sf_133)
    assert sf_133.account_num == tas.account_num


def test_load_sf133_local(database):
    """ This function loads the data from a local file to the database """
    sess = database.session
    sf133_path = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'sf_133_2021_05.csv')

    load_sf133.load_sf133(sess, sf133_path, 2021, 5)

    # We should have loaded twelve rows and the duplicated Q/QQQ rows should have been combined
    assert sess.query(SF133).count() == 12

    # Picking one of the lines to do spot checks on
    single_check = sess.query(SF133).filter_by(line=1021, disaster_emergency_fund_code='Q').one()
    assert single_check.display_tas == '005-X-0107-000'
    assert single_check.amount == Decimal('97515.05')
    assert single_check.fiscal_year == 2021
    assert single_check.period == 5

    # Checking that DEFC that wasn't listed for one line got filled in with a 0
    single_check = sess.query(SF133).filter_by(line=1021, disaster_emergency_fund_code='E').one()
    assert single_check.display_tas == '005-X-0107-000'
    assert single_check.amount == 0
    assert single_check.fiscal_year == 2021
    assert single_check.period == 5

    # Checking on duplicate DEFC (one Q and one QQQ for TAS being equal). For now takes an average, should be sum
    # Have to use "round" because Decimals do stupid things
    dupe_check = sess.query(SF133).filter_by(line=1070, disaster_emergency_fund_code='Q').one()
    # TODO: Uncomment this check once the fix is in, this is the actual sum total
    # assert round(dupe_check.amount, 2) == Decimal('62631744.02')
    assert round(dupe_check.amount, 2) == Decimal('31315872.01')
