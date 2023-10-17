from io import StringIO

from dataactvalidator.scripts import read_zips
from dataactvalidator.scripts.read_zips import (
    generate_zips_grouped, generate_cd_state_grouped, generate_cd_zips_grouped,
    generate_cd_county_grouped, update_state_congr_table_census
)
from dataactvalidator.scripts import load_location_data
from dataactvalidator.scripts.load_location_data import generate_cd_city_grouped
from dataactcore.models.domainModels import (
    CDCityGrouped, CDCountyGrouped, CDStateGrouped, CDZipsGrouped, StateCongressional, Zips, ZipsGrouped, ZipCity
)


def test_parse_census_district_file(database):
    census_file_mock = StringIO("""state_code,congressional_district_no,census_year\nA,1,20""")
    sess = database.session

    update_state_congr_table_census(census_file_mock, sess)

    state_congressional = sess.query(StateCongressional).one_or_none()
    assert state_congressional.state_code == 'A'
    assert state_congressional.congressional_district_no == '01'
    assert state_congressional.census_year == 20


def test_group_zips(database, monkeypatch):
    """ Testing the grouping of zips. """
    sess = database.session

    # Testing with a threshold of a simple majority
    test_threshold = 0.51
    monkeypatch.setattr(read_zips, 'MULTIPLE_LOCATION_THRESHOLD_PERCENTAGE', test_threshold)
    monkeypatch.setattr(load_location_data, 'MULTIPLE_LOCATION_THRESHOLD_PERCENTAGE', test_threshold)

    test_data = []

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

    test_data.extend([zip_same1, zip_same2, zip_state1, zip_state2, zip_county1, zip_county2, zip_cd1, zip_cd2,
                      zip_state_cd1, zip_state_cd2, zip_null_cd])

    # cd_state_grouped data - group by state, threshold overwritten to 100%
    # Split among 3, 01 having 66% => 90
    cd_state_grouped_thirds_1 = Zips(zip5='00000', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                     congressional_district_no='01')
    cd_state_grouped_thirds_2 = Zips(zip5='00000', zip_last4='0002', state_abbreviation='AA', county_number='000',
                                     congressional_district_no='01')
    cd_state_grouped_thirds_3 = Zips(zip5='00000', zip_last4='0003', state_abbreviation='AA', county_number='000',
                                     congressional_district_no='02')
    cd_state_grouped_thirds_null = Zips(zip5='00000', zip_last4='0004', state_abbreviation='AA', county_number='000',
                                        congressional_district_no=None)
    # Split among 2, 50% => 90
    cd_state_grouped_half_1 = Zips(zip5='00001', zip_last4='0001', state_abbreviation='AB', county_number='000',
                                   congressional_district_no='01')
    cd_state_grouped_half_2 = Zips(zip5='00001', zip_last4='0002', state_abbreviation='AB', county_number='000',
                                   congressional_district_no='01')
    # Not split, 100% => 01
    cd_state_grouped_match = Zips(zip5='00002', zip_last4='0001', state_abbreviation='AC', county_number='000',
                                  congressional_district_no='01')
    test_data.extend([cd_state_grouped_match, cd_state_grouped_half_1, cd_state_grouped_half_2,
                      cd_state_grouped_thirds_1, cd_state_grouped_thirds_2, cd_state_grouped_thirds_3,
                      cd_state_grouped_thirds_null])

    # cd_zips_grouped data - group by state + zip
    # Split among 3, 01 having 66% => 01
    cd_zips_grouped_thirds_1 = Zips(zip5='00003', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='01')
    cd_zips_grouped_thirds_2 = Zips(zip5='00003', zip_last4='0002', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='01')
    cd_zips_grouped_thirds_3 = Zips(zip5='00003', zip_last4='0003', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='02')
    cd_zips_grouped_thirds_null = Zips(zip5='00003', zip_last4='0004', state_abbreviation='AA', county_number='000',
                                       congressional_district_no=None)
    # Split among 2, 50% => 90
    cd_zips_grouped_half_1 = Zips(zip5='00004', zip_last4='0001', state_abbreviation='AB', county_number='000',
                                  congressional_district_no='01')
    cd_zips_grouped_half_2 = Zips(zip5='00004', zip_last4='0002', state_abbreviation='AB', county_number='000',
                                  congressional_district_no='02')
    # Not split, 100% => 01
    cd_zips_grouped_match = Zips(zip5='00005', zip_last4='0001', state_abbreviation='AC', county_number='000',
                                 congressional_district_no='01')
    test_data.extend([cd_zips_grouped_match, cd_zips_grouped_half_1, cd_zips_grouped_half_2, cd_zips_grouped_thirds_1,
                      cd_zips_grouped_thirds_2, cd_zips_grouped_thirds_3, cd_zips_grouped_thirds_null])

    # cd_city_grouped data - group by state + city
    # Split among 3, 01 having 66% => 01
    cd_city_grouped_thirds_1 = Zips(zip5='00006', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='01')
    cd_city_grouped_thirds_2 = Zips(zip5='00006', zip_last4='0002', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='01')
    cd_city_grouped_thirds_3 = Zips(zip5='00006', zip_last4='0003', state_abbreviation='AA', county_number='000',
                                    congressional_district_no='02')
    cd_city_grouped_thirds_null = Zips(zip5='00006', zip_last4='0004', state_abbreviation='AA', county_number='000',
                                       congressional_district_no=None)
    cd_zip_city_1 = ZipCity(zip_code='00006', state_code='AA', city_name='Test City 1')
    # Split among 2, 50% => 90
    cd_city_grouped_half_1 = Zips(zip5='00007', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                  congressional_district_no='01')
    cd_city_grouped_half_2 = Zips(zip5='00007', zip_last4='0002', state_abbreviation='AA', county_number='000',
                                  congressional_district_no='02')
    cd_zip_city_2 = ZipCity(zip_code='00007', state_code='AA', city_name='Test City 2')
    # Not split, 100% => 01
    cd_city_grouped_match = Zips(zip5='00008', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                 congressional_district_no='01')
    cd_zip_city_3 = ZipCity(zip_code='00008', state_code='AA', city_name='Test City 3')
    test_data.extend([cd_city_grouped_match, cd_city_grouped_half_1, cd_city_grouped_half_2, cd_city_grouped_thirds_1,
                      cd_city_grouped_thirds_2, cd_city_grouped_thirds_3, cd_zip_city_1, cd_zip_city_2, cd_zip_city_3,
                      cd_city_grouped_thirds_null])

    # cd_county_grouped data - group by state + county
    # Split among 3, 01 having 66% => 01
    cd_county_grouped_thirds_1 = Zips(zip5='00009', zip_last4='0001', state_abbreviation='AA', county_number='000',
                                      congressional_district_no='01')
    cd_county_grouped_thirds_2 = Zips(zip5='00009', zip_last4='0002', state_abbreviation='AA', county_number='000',
                                      congressional_district_no='01')
    cd_county_grouped_thirds_3 = Zips(zip5='00009', zip_last4='0003', state_abbreviation='AA', county_number='000',
                                      congressional_district_no='02')
    cd_county_grouped_thirds_null = Zips(zip5='00009', zip_last4='0004', state_abbreviation='AA', county_number='000',
                                         congressional_district_no=None)
    # Split among 2, 50% => 90
    cd_county_grouped_half_1 = Zips(zip5='00010', zip_last4='0001', state_abbreviation='AA', county_number='001',
                                    congressional_district_no='01')
    cd_county_grouped_half_2 = Zips(zip5='00010', zip_last4='0002', state_abbreviation='AA', county_number='001',
                                    congressional_district_no='02')
    # Not split, 100% => 01
    cd_county_grouped_match = Zips(zip5='00011', zip_last4='0001', state_abbreviation='AA', county_number='002',
                                   congressional_district_no='01')
    test_data.extend([cd_county_grouped_match, cd_county_grouped_half_1, cd_county_grouped_half_2,
                      cd_county_grouped_thirds_1, cd_county_grouped_thirds_2, cd_county_grouped_thirds_3,
                      cd_county_grouped_thirds_null])

    sess.add_all(test_data)
    sess.commit()

    tables = [CDStateGrouped, CDZipsGrouped, CDCityGrouped, CDCountyGrouped, ZipsGrouped]
    tables = [table.__table__ for table in tables]
    # Creating the temp tables to use for testing
    sess.execute("""
        CREATE TABLE temp_zips AS
        SELECT * FROM zips;
        CREATE TABLE temp_zip_city AS
        SELECT * FROM zip_city;
    """)
    for table in tables:
        sess.execute(f"CREATE TABLE temp_{table} (LIKE {table} INCLUDING ALL);")
    sess.commit()

    # Populate the tables
    generate_zips_grouped(sess)
    generate_cd_state_grouped(sess)
    generate_cd_zips_grouped(sess)
    generate_cd_county_grouped(sess)
    generate_cd_city_grouped(sess)

    # Moving into zips_grouped for easier parsing
    for table in tables:
        sess.execute(f"""
            INSERT INTO {table}
            SELECT *
            FROM temp_{table}
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

    # CDStateGrouped
    # Thirds - threshold overwritten to 100%
    cds = sess.query(CDStateGrouped).filter_by(state_abbreviation=cd_state_grouped_thirds_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == '90'
    # Half
    cds = sess.query(CDStateGrouped).filter_by(state_abbreviation=cd_state_grouped_half_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == '90'
    # Match
    cds = sess.query(CDStateGrouped).filter_by(state_abbreviation=cd_state_grouped_match.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_state_grouped_match.congressional_district_no

    # CDZipsGrouped
    # Thirds
    cds = sess.query(CDZipsGrouped).filter_by(zip5=cd_zips_grouped_thirds_1.zip5,
                                              state_abbreviation=cd_zips_grouped_thirds_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_zips_grouped_thirds_1.congressional_district_no
    # Half
    cds = sess.query(CDZipsGrouped).filter_by(zip5=cd_zips_grouped_half_1.zip5,
                                              state_abbreviation=cd_zips_grouped_half_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == '90'
    # Match
    cds = sess.query(CDZipsGrouped).filter_by(zip5=cd_zips_grouped_match.zip5,
                                              state_abbreviation=cd_zips_grouped_match.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_state_grouped_match.congressional_district_no

    # CDCityGrouped
    # Thirds
    cds = sess.query(CDCityGrouped).filter_by(city_name=cd_zip_city_1.city_name,
                                              state_abbreviation=cd_city_grouped_thirds_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_city_grouped_thirds_1.congressional_district_no
    # Half
    cds = sess.query(CDCityGrouped).filter_by(city_name=cd_zip_city_2.city_name,
                                              state_abbreviation=cd_city_grouped_half_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == '90'
    # Match
    cds = sess.query(CDCityGrouped).filter_by(city_name=cd_zip_city_3.city_name,
                                              state_abbreviation=cd_city_grouped_match.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_city_grouped_match.congressional_district_no

    # CDCountyGrouped
    # Thirds
    cds = sess.query(CDCountyGrouped).filter_by(county_number=cd_county_grouped_thirds_1.county_number,
                                                state_abbreviation=cd_county_grouped_thirds_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_county_grouped_thirds_1.congressional_district_no
    # Half
    cds = sess.query(CDCountyGrouped).filter_by(county_number=cd_county_grouped_half_1.county_number,
                                                state_abbreviation=cd_county_grouped_half_1.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == '90'
    # Match
    cds = sess.query(CDCountyGrouped).filter_by(county_number=cd_county_grouped_match.county_number,
                                                state_abbreviation=cd_county_grouped_match.state_abbreviation).all()
    assert len(cds) == 1
    assert cds[0].congressional_district_no == cd_county_grouped_match.congressional_district_no
