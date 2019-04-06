from io import StringIO

from dataactvalidator.scripts.read_zips import update_state_congr_table_census
from dataactcore.models.domainModels import StateCongressional


def test_parse_census_district_file(database):
    census_file_mock = StringIO("""state_code,congressional_district_no,census_year\nA,1,20""")
    sess = database.session

    update_state_congr_table_census(census_file_mock, sess)

    state_congressional = sess.query(StateCongressional).one_or_none()
    assert(state_congressional.state_code == 'A')
    assert(state_congressional.congressional_district_no == '01')
    assert (state_congressional.census_year == 20)
