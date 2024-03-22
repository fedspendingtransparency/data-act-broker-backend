import os

from decimal import Decimal

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import GTASBOC
from dataactcore.scripts.pipeline import load_gtas_boc


def test_load_sf133_local(database):
    """ This function loads the data from a local file to the database """
    sess = database.session
    sf133_path = os.path.join(CONFIG_BROKER['path'], 'tests', 'unit', 'data', 'OMB_Extract_BOC_2023_09.txt')

    load_gtas_boc.load_boc(sess, sf133_path, 2021, 5)

    # We should have loaded twelve rows and the duplicated Q/QQQ rows should have been combined
    assert sess.query(GTASBOC).count() == 6

    # Picking one of the lines to do spot checks on
    single_check = sess.query(GTASBOC).filter_by(agency_identifier='025').one()
    assert single_check.display_tas == '025-X-0107-000'
    assert single_check.dollar_amount == Decimal('474.64')
    assert single_check.fiscal_year == 2023
    assert single_check.period == 9

    # Checking that DEFC QQQ got renamed to Q
    dupe_check = sess.query(GTASBOC).filter_by(disaster_emergency_fund_code='Q').one()
    assert dupe_check.amount == Decimal('21409.92')