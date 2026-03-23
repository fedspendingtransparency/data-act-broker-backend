import os
from datetime import date
from decimal import Decimal

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SF133
from dataactcore.scripts.pipeline import load_sf133
from tests.unit.dataactcore.factories.domain import SF133Factory, TASFactory


def test_update_account_nums_fiscal_year(database):
    """Fiscal year math should be accurate when checking TAS entries"""
    sess = database.session
    tas = TASFactory(internal_start_date=date(2010, 1, 1), internal_end_date=date(2010, 8, 31))
    sf_133 = SF133Factory(fiscal_year=2011, period=1, **tas.component_dict(), tas=tas.tas)
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
    """This function loads the data from a local file to the database"""
    sess = database.session
    sf133_path = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "sf_133_2021_05.csv")

    load_sf133.load_sf133(sess, sf133_path, 2021, 5)

    # We should have loaded eight (nine before combining) rows and the duplicated Q/QQQ rows should have been combined
    assert sess.query(SF133).count() == 8

    # Picking one of the lines to do spot checks on
    single_check = sess.query(SF133).filter_by(line=1021, disaster_emergency_fund_code="Q").one()
    assert single_check.display_tas == "005-X-0107-000"
    assert single_check.amount == Decimal("97515.05")
    assert single_check.fiscal_year == 2021
    assert single_check.period == 5

    # Checking that DEFC that wasn't listed for one line did not get added
    single_check = sess.query(SF133).filter_by(line=1021, disaster_emergency_fund_code="E").one_or_none()
    assert single_check is None

    # Checking on duplicate DEFC (one Q and one QQQ for TAS being equal). For now takes an average, should be sum
    # Have to use "round" because Decimals do stupid things
    dupe_check = sess.query(SF133).filter_by(line=1070, disaster_emergency_fund_code="Q").one()
    assert dupe_check.amount == Decimal("62631744.02")
