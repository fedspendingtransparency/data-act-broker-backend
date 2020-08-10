from io import StringIO

from dataactvalidator.scripts.read_zips import update_state_congr_table_census, group_zips
from dataactcore.models.domainModels import StateCongressional, Zips, ZipsGrouped


def test_parse_census_district_file(database):
    census_file_mock = StringIO("""state_code,congressional_district_no,census_year\nA,1,20""")
    sess = database.session

    update_state_congr_table_census(census_file_mock, sess)

    state_congressional = sess.query(StateCongressional).one_or_none()
    assert state_congressional.state_code == 'A'
    assert state_congressional.congressional_district_no == '01'
    assert state_congressional.census_year == 20


def test_group_zips(database):
    """ Testing the grouping of zips. """
    sess = database.session
    # Only difference is the zip_last4, these will be merged together
    zip_same1 = Zips(zip5='12345', zip_last4='6789', state_abbreviation='VA', county_number='000',
                     congressional_district_no='01')
    zip_same2 = Zips(zip5='12345', zip_last4='6780', state_abbreviation='VA', county_number='000',
                     congressional_district_no='01')

    # Different states, same everything else
    zip_state1 = Zips(zip5='54321', zip_last4='6789', state_abbreviation='VA', county_number='000',
                      congressional_district_no='01')
    zip_state2 = Zips(zip5='54321', zip_last4='6780', state_abbreviation='WA', county_number='000',
                      congressional_district_no='01')

    # Different county codes, same everything else
    zip_county1 = Zips(zip5='11111', zip_last4='1111', state_abbreviation='VA', county_number='000',
                       congressional_district_no='01')
    zip_county2 = Zips(zip5='11111', zip_last4='1112', state_abbreviation='VA', county_number='001',
                       congressional_district_no='01')

    # Everything matches except for congressional district
    zip_cd1 = Zips(zip5='22222', zip_last4='2222', state_abbreviation='VA', county_number='000',
                   congressional_district_no='01')
    zip_cd2 = Zips(zip5='22222', zip_last4='2223', state_abbreviation='VA', county_number='000',
                   congressional_district_no='02')

    # Different states, different congressional district
    zip_state_cd1 = Zips(zip5='33333', zip_last4='3333', state_abbreviation='VA', county_number='000',
                         congressional_district_no='01')
    zip_state_cd2 = Zips(zip5='33333', zip_last4='3334', state_abbreviation='WA', county_number='000',
                         congressional_district_no='02')

    # Null congressional district
    zip_null_cd = Zips(zip5='44444', zip_last4='4444', state_abbreviation='WA', county_number='000',
                       congressional_district_no=None)

    sess.add_all([zip_same1, zip_same2, zip_state1, zip_state2, zip_county1, zip_county2, zip_cd1, zip_cd2,
                  zip_state_cd1, zip_state_cd2, zip_null_cd])
    sess.commit()

    # Creating the temp tables to use for testing
    sess.execute("""
        CREATE TABLE temp_zips AS
        SELECT * FROM zips;

        CREATE TABLE temp_zips_grouped (LIKE zips_grouped INCLUDING ALL);
    """)
    sess.commit()

    group_zips(sess)

    # Moving into zips_grouped for easier parsing
    sess.execute("""
        INSERT INTO zips_grouped
        SELECT *
        FROM temp_zips_grouped
    """)
    sess.commit()

    # Combined first set of zips
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_same1.zip5).all()
    assert len(zips) == 1
    assert zips[0].zip5 == zip_same1.zip5
    assert zips[0].state_abbreviation == zip_same1.state_abbreviation
    assert zips[0].county_number == zip_same1.county_number
    assert zips[0].congressional_district_no == zip_same1.congressional_district_no

    # Different states, same everything else
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_state1.zip5).order_by(ZipsGrouped.state_abbreviation).all()
    assert len(zips) == 2
    assert zips[0].zip5 == zip_state1.zip5
    assert zips[0].state_abbreviation == zip_state1.state_abbreviation
    assert zips[0].county_number == zip_state1.county_number
    assert zips[0].congressional_district_no == zip_state1.congressional_district_no
    assert zips[1].zip5 == zip_state2.zip5
    assert zips[1].state_abbreviation == zip_state2.state_abbreviation
    assert zips[1].county_number == zip_state2.county_number
    assert zips[1].congressional_district_no == zip_state2.congressional_district_no

    # Different counties, same everything else
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_county1.zip5).order_by(ZipsGrouped.county_number).all()
    assert len(zips) == 2
    assert zips[0].zip5 == zip_county1.zip5
    assert zips[0].state_abbreviation == zip_county1.state_abbreviation
    assert zips[0].county_number == zip_county1.county_number
    assert zips[0].congressional_district_no == zip_county1.congressional_district_no
    assert zips[1].zip5 == zip_county2.zip5
    assert zips[1].state_abbreviation == zip_county2.state_abbreviation
    assert zips[1].county_number == zip_county2.county_number
    assert zips[1].congressional_district_no == zip_county2.congressional_district_no

    # Different congressional districts
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_cd1.zip5).all()
    assert len(zips) == 1
    assert zips[0].zip5 == zip_cd1.zip5
    assert zips[0].state_abbreviation == zip_cd1.state_abbreviation
    assert zips[0].county_number == zip_cd1.county_number
    assert zips[0].congressional_district_no == '90'

    # Different states, different congressional districts
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_state_cd1.zip5).order_by(ZipsGrouped.state_abbreviation).all()
    assert len(zips) == 2
    assert zips[0].zip5 == zip_state_cd1.zip5
    assert zips[0].state_abbreviation == zip_state_cd1.state_abbreviation
    assert zips[0].county_number == zip_state_cd1.county_number
    assert zips[0].congressional_district_no == '90'
    assert zips[1].zip5 == zip_state_cd2.zip5
    assert zips[1].state_abbreviation == zip_state_cd2.state_abbreviation
    assert zips[1].county_number == zip_state_cd2.county_number
    assert zips[1].congressional_district_no == '90'

    # Null congressional district
    zips = sess.query(ZipsGrouped).filter_by(zip5=zip_null_cd.zip5).all()
    assert len(zips) == 1
    assert zips[0].zip5 == zip_null_cd.zip5
    assert zips[0].state_abbreviation == zip_null_cd.state_abbreviation
    assert zips[0].county_number == zip_null_cd.county_number
    assert zips[0].congressional_district_no == '90'
