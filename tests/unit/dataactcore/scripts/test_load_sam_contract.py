import os
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SubTierAgency, CGAC, FREC, Zips, ZipsGrouped, CountryCode, States, CountyCode, SAMRecipient
from dataactcore.models.stagingModels import DetachedAwardProcurement

from dataactcore.scripts.pipeline import load_sam_contract

def remove_metrics_file():
    if os.path.isfile("load_sam_contract_metrics.json"):
        os.remove("load_sam_contract_metrics.json")


def prep_data(sess):
    """Prepare the contracts data and relevant dataframes for testing"""
    # Add zips data to database
    zip_code1 = Zips(zip5="12345", zip_last4="6789", county_number="000")
    zip_code2 = Zips(zip5="54321", zip_last4=None, county_number="001")

    zips_grouped = ZipsGrouped(zip5 = "12345", county_number="002")

    # Add agency data to the database
    cgac1 = CGAC(cgac_id=1, cgac_code="170", agency_name="test CGAC")
    cgac2 = CGAC(cgac_id=2, cgac_code="123", agency_name="FREC CGAC")
    frec = FREC(frec_id=1, cgac_id=2, frec_code="1234", agency_name="test FREC")
    sub_tier1 = SubTierAgency(sub_tier_agency_code="0001", cgac_id=1, frec_id=1, sub_tier_agency_name="CGAC SUBTIER", is_frec=False)
    sub_tier2 = SubTierAgency(sub_tier_agency_code="0002", cgac_id=2, frec_id=1, sub_tier_agency_name="FREC SUBTIER",
                              is_frec=True)

    # Add country data to the database
    country1 = CountryCode(country_code="USA", country_name="UNITED STATES")
    country2 = CountryCode(country_code="KEN", country_name="KENYA")

    # Add state data to the database
    states1 = States(state_code="KS", state_name="Kansas")
    states2 = States(state_code="VA", state_name="Virginia")
    states3 = States(state_code="GU", state_name="Guam")

    # Add county data to the database
    county1 = CountyCode(county_number="000", county_name="Test Name", state_code="AL")
    county2 = CountyCode(county_number="000", county_name=" Test Name VA 000", state_code="VA")
    county3 = CountyCode(county_number="049", county_name="A Derived Name (CA)", state_code="KS")

    # Add SAM Recipient data to the database
    sam1 = SAMRecipient(uei="ABCDEFG", high_comp_officer1_full_na="Name 1", high_comp_officer1_amount="2",
        high_comp_officer2_full_na="Name 2", high_comp_officer2_amount="5",
        high_comp_officer3_full_na="Name 3", high_comp_officer3_amount="14",
        high_comp_officer4_full_na="Name 4", high_comp_officer4_amount="9",
        high_comp_officer5_full_na="Name 5", high_comp_officer5_amount="11")
    sam2 = SAMRecipient(uei="ZYXWVUT", high_comp_officer1_full_na="Name 1a", high_comp_officer1_amount="10",
        high_comp_officer2_full_na="Name 2a", high_comp_officer2_amount="7",
        high_comp_officer3_full_na=None, high_comp_officer3_amount=None,
        high_comp_officer4_full_na=None, high_comp_officer4_amount=None,
        high_comp_officer5_full_na=None, high_comp_officer5_amount=None)
    sess.add_all([zip_code1, zip_code2, zips_grouped, cgac1, cgac2, frec, sub_tier1, sub_tier2, country1, country2,
                  states1, states2, states3, county1, county2, county3, sam1, sam2])
    sess.commit()


def get_file(contract_type):
    """Get test file based on contract_type"""
    # Get the test file
    contract_file = os.path.join(CONFIG_BROKER["path"], "tests", "unit", "data", "fake_sam_files", "contract", f"sam_contract_{contract_type}.csv")
    contract_data = pd.read_csv(contract_file, dtype=str)
    return contract_data


def test_create_lookups(database):
    """Test the creation of lookups to make sure they are acting as we expect"""
    sess = database.session
    prep_data(sess)
    sub_tier_df, country_df, state_df, county_df, exec_comp_df = load_sam_contract.create_lookups(sess)
    # Subtier
    expected_sub_tier_df = pd.DataFrame({"sub_tier_agency_c": ["0001", "0002"],
                                         "agency_code": ["170", "1234"],
                                         "agency_name": ["test CGAC", "test FREC"]})
    pd.testing.assert_frame_equal(sub_tier_df, expected_sub_tier_df)

    # Country
    expected_country_df = pd.DataFrame({"country_code": ["USA", "KEN"],
                                         "country_name": ["UNITED STATES", "KENYA"]})
    pd.testing.assert_frame_equal(country_df, expected_country_df)

    # State: capitalizing names
    expected_state_df = pd.DataFrame({"state_code": ["KS", "VA", "GU"],
                                        "state_name": ["KANSAS", "VIRGINIA", "GUAM"]})
    pd.testing.assert_frame_equal(state_df, expected_state_df)

    # County: capitalizing names and removing " (CA)" and trimming
    expected_county_df = pd.DataFrame({"county_number": ["000", "000", "049"],
                                      "state_code": ["AL", "VA", "KS"],
                                       "county_name": ["TEST NAME", "TEST NAME VA 000", "A DERIVED NAME"]})
    pd.testing.assert_frame_equal(county_df, expected_county_df)

    # Executive compensation information
    expected_exec_comp_df = pd.DataFrame({"high_comp_officer1_full_na": ["Name 1", "Name 1a"],
                                       "high_comp_officer1_amount": ["2", "10"],
                                       "high_comp_officer2_full_na": ["Name 2", "Name 2a"],
                                          "high_comp_officer2_amount": ["5", "7"],
                                          "high_comp_officer3_full_na": ["Name 3", None],
                                          "high_comp_officer3_amount": ["14", None],
                                          "high_comp_officer4_full_na": ["Name 4", None],
                                          "high_comp_officer4_amount": ["9", None],
                                          "high_comp_officer5_full_na": ["Name 5", None],
                                          "high_comp_officer5_amount": ["11", None],
                                          "uei": ["ABCDEFG", "ZYXWVUT"]})
    pd.testing.assert_frame_equal(exec_comp_df, expected_exec_comp_df)

    remove_metrics_file()


def test_calculate_ppop_fields(database):
    """Test that calculate_ppop_fields properly derives the relevant fields"""
    sess = database.session
    prep_data(sess)
    contract_data = get_file("award")
    sub_tier_df, country_df, state_df, county_df, exec_comp_df = load_sam_contract.create_lookups(sess)
    load_sam_contract.process_data(contract_data,
        "award",
        sess,
        sub_tier_df,
        county_df,
        state_df,
        country_df,
        exec_comp_df)
    
    row1 = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique="1234_-none-_ABCPIID1_0101_-none-_4").one_or_none()
    row2 = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique="1234_-none-_ABCPIID2_ABC32_-none-_0").one_or_none()
    row3 = sess.query(DetachedAwardProcurement).filter_by(detached_award_proc_unique="1234_-none-_ABCPIID3_0_-none-_1").one_or_none()
    row4 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID4_0_-none-_0").one_or_none()
    row5 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID5_-none-_-none-_0").one_or_none()


    # 9-digit zip matches DB for county
    assert row1.place_of_perform_country_c == "USA"
    assert row1.place_of_perf_country_desc == "UNITED STATES"
    assert row1.place_of_performance_state == "AL"
    assert row1.place_of_perfor_state_desc == "ALABAMA"
    assert row1.place_of_performance_zip5 == "12345"
    assert row1.place_of_perform_zip_last4 == "6789"
    assert row1.place_of_perform_county_co == "000"
    assert row1.place_of_perform_county_na == "TEST NAME"

    assert row2.place_of_perform_country_c == "KEN"
    assert row2.place_of_perf_country_desc == "KENYA"
    assert row2.place_of_performance_state is None
    assert row2.place_of_perfor_state_desc is None
    # Don't do any zip-based derivations because this isn't from the US
    assert row2.place_of_performance_zip5 is None
    assert row2.place_of_perform_zip_last4 is None
    assert row2.place_of_perform_county_co is None
    assert row2.place_of_perform_county_na is None

    # Country started as GUAM, derived to USA and moved to state regardless of original state
    assert row3.place_of_perform_country_c == "USA"
    assert row3.place_of_perf_country_desc == "UNITED STATES"
    assert row3.place_of_performance_state == "GU"
    assert row3.place_of_perfor_state_desc == "GUAM"
    assert row3.place_of_performance_zip5 == "12346"
    assert row3.place_of_perform_zip_last4 is None
    assert row3.place_of_perform_county_co is None
    assert row3.place_of_perform_county_na is None

    # 5-digit zip matches other in DB
    assert row4.place_of_perform_country_c == "USA"
    assert row4.place_of_perf_country_desc == "UNITED STATES"
    assert row4.place_of_performance_state == "KS"
    assert row4.place_of_perfor_state_desc == "KANSAS"
    assert row4.place_of_performance_zip5 == "12345"
    assert row4.place_of_perform_zip_last4 is None
    # No associated county for Kansas so while we could get the code from the zip we don't have a name
    assert row4.place_of_perform_county_co == "002"
    assert row4.place_of_perform_county_na is None

    # Don't derive state or county name if given and not a territory situation, even if it's wrong
    assert row5.place_of_performance_state == "KS"
    assert row5.place_of_perfor_state_desc == "WRONG"
    # Padding the county provided to 3 digits
    assert row5.place_of_perform_county_co == "049"
    assert row5.place_of_perform_county_na == "POTTAWATOMIE"

    remove_metrics_file()


def test_calculate_legal_entity_fields(database):
    """Test that calculate_legal_entity_fields properly derives the relevant fields"""
    sess = database.session
    prep_data(sess)
    contract_data = get_file("award")
    sub_tier_df, country_df, state_df, county_df, exec_comp_df = load_sam_contract.create_lookups(sess)
    load_sam_contract.process_data(contract_data,
                                   "award",
                                   sess,
                                   sub_tier_df,
                                   county_df,
                                   state_df,
                                   country_df,
                                   exec_comp_df)

    row1 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID1_0101_-none-_4").one_or_none()
    row2 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID2_ABC32_-none-_0").one_or_none()
    row3 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID3_0_-none-_1").one_or_none()
    row4 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID4_0_-none-_0").one_or_none()
    row5 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID5_-none-_-none-_0").one_or_none()

    # 9-digit zip matches the one in the DB
    assert row1.legal_entity_country_code == "USA"
    assert row1.legal_entity_country_name == "UNITED STATES"
    assert row1.legal_entity_state_code == "VA"
    assert row1.legal_entity_state_descrip == "VIRGINIA"
    assert row1.legal_entity_zip5 == "12345"
    assert row1.legal_entity_zip_last4 == "6789"
    assert row1.legal_entity_county_code == "000"
    assert row1.legal_entity_county_name == "TEST NAME VA 000"

    assert row2.legal_entity_country_code == "USA"
    assert row2.legal_entity_country_name == "UNITED STATES"
    assert row2.legal_entity_state_code == "GU"
    assert row2.legal_entity_state_descrip == "GUAM"
    assert row2.legal_entity_zip5 == "12346"
    assert row2.legal_entity_zip_last4 is None
    # Nothing in zips_grouped for the 5-digit code and provided state so no county is derived
    assert row2.legal_entity_county_code is None
    assert row2.legal_entity_county_name is None

    assert row3.legal_entity_country_code == "KEN"
    assert row3.legal_entity_country_name == "KENYA"
    assert row3.legal_entity_state_code is None
    assert row3.legal_entity_state_descrip is None
    # Not deriving zip-based data because it is a foreign location
    assert row3.legal_entity_zip5 is None
    assert row3.legal_entity_zip_last4 is None
    assert row3.legal_entity_county_code is None
    assert row3.legal_entity_county_name is None

    assert row4.legal_entity_country_code == "USA"
    assert row4.legal_entity_country_name == "UNITED STATES"
    assert row4.legal_entity_state_code == "VA"
    # State name derived because it's blank
    assert row4.legal_entity_state_descrip == "VIRGINIA"
    assert row4.legal_entity_zip5 == "12345"
    assert row4.legal_entity_zip_last4 is None
    # No associated county for Virginia so while we could get the code from the zip we don't have a name
    assert row4.legal_entity_county_code == "002"
    assert row4.legal_entity_county_name is None

    # Don't derive state name if given and not a territory situation, even if it's wrong
    assert row5.legal_entity_state_code == "VA"
    assert row5.legal_entity_state_descrip == "CHANGED"

    remove_metrics_file()


def test_derive_remaining_fields(database):
    """Test that derive_remaining_fields properly derives the relevant fields (ignores ppop and le because other tests deal with those)"""
    sess = database.session
    prep_data(sess)
    contract_data_award = get_file("award")
    contract_data_idv = get_file("idv")
    sub_tier_df, country_df, state_df, county_df, exec_comp_df = load_sam_contract.create_lookups(sess)
    load_sam_contract.process_data(contract_data_award,
                                   "award",
                                   sess,
                                   sub_tier_df,
                                   county_df,
                                   state_df,
                                   country_df,
                                   exec_comp_df)
    load_sam_contract.process_data(contract_data_idv,
                                   "idv",
                                   sess,
                                   sub_tier_df,
                                   county_df,
                                   state_df,
                                   country_df,
                                   exec_comp_df)

    row1 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID1_0101_-none-_4").one_or_none()
    row2 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID2_ABC32_-none-_0").one_or_none()
    row3 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID3_0_-none-_1").one_or_none()
    row4 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_ABCPIID4_0_-none-_0").one_or_none()
    # IDV row
    row5 = sess.query(DetachedAwardProcurement).filter_by(
        detached_award_proc_unique="1234_-none-_CONTPIID1_NUM001_-none-_-none-").one_or_none()

    assert row1.awarding_agency_code == "170"
    assert row1.awarding_agency_name == "test CGAC"
    assert row1.funding_agency_code == "1234"
    assert row1.funding_agency_name == "test FREC"
    assert row1.city_local_government is True
    # This is not a test to see if the business categories function works, that is tested elsewhere, this is only
    # testing to make sure it got put in the right place
    assert sorted(row1.business_categories) == ['category_business', 'corporate_entity_not_tax_exempt', 'government', 'local_government', 'manufacturer_of_goods', 'other_than_small_business', 'special_designations', 'us_owned_business']
    assert row1.high_comp_officer1_full_na == "Name 1"
    assert row1.high_comp_officer1_amount == "2"
    assert row1.high_comp_officer2_full_na == "Name 2"
    assert row1.high_comp_officer2_amount == "5"
    assert row1.high_comp_officer3_full_na == "Name 3"
    assert row1.high_comp_officer3_amount == "14"
    assert row1.high_comp_officer4_full_na == "Name 4"
    assert row1.high_comp_officer4_amount == "9"
    assert row1.high_comp_officer5_full_na == "Name 5"
    assert row1.high_comp_officer5_amount == "11"
    assert row1.additional_reporting == "NONE: NONE OF THE ABOVE"
    # vendor_legal_org_name should be the same as uei_legal_business_name because they've been combined into one
    assert row1.vendor_legal_org_name == row1.uei_legal_business_name
    assert row1.unique_award_key == "CONT_AWD_ABCPIID1_1234_-NONE-_-NONE-"

    assert row2.awarding_agency_code == "1234"
    assert row2.awarding_agency_name == "test FREC"
    assert row2.funding_agency_code == "999"
    assert row2.funding_agency_name is None
    assert row2.city_local_government is False
    assert row2.high_comp_officer1_full_na == "Name 1a"
    assert row2.high_comp_officer1_amount == "10"
    assert row2.high_comp_officer2_full_na == "Name 2a"
    assert row2.high_comp_officer2_amount == "7"
    assert row2.high_comp_officer3_full_na is None
    assert row2.high_comp_officer3_amount is None
    assert row2.high_comp_officer4_full_na is None
    assert row2.high_comp_officer4_amount is None
    assert row2.high_comp_officer5_full_na is None
    assert row2.high_comp_officer5_amount is None
    assert row2.unique_award_key == "CONT_AWD_ABCPIID2_1234_-NONE-_-NONE-"

    assert row3.awarding_agency_code == "999"
    assert row3.awarding_agency_name is None
    assert row3.funding_agency_code == "170"
    assert row3.funding_agency_name == "test CGAC"
    # boolean column entry was blank, converted to False
    assert row3.city_local_government is False
    # UEI didn't match anything in the DB, no deriving
    assert row3.high_comp_officer1_full_na is None
    assert row3.high_comp_officer1_amount is None
    assert row3.high_comp_officer2_full_na is None
    assert row3.high_comp_officer2_amount is None
    assert row3.high_comp_officer3_full_na is None
    assert row3.high_comp_officer3_amount is None
    assert row3.high_comp_officer4_full_na is None
    assert row3.high_comp_officer4_amount is None
    assert row3.high_comp_officer5_full_na is None
    assert row3.high_comp_officer5_amount is None
    assert row3.unique_award_key == "CONT_AWD_ABCPIID3_1234_-NONE-_-NONE-"

    # No UEI provided
    assert row4.high_comp_officer1_full_na is None
    assert row4.high_comp_officer1_amount is None
    assert row4.high_comp_officer2_full_na is None
    assert row4.high_comp_officer2_amount is None
    assert row4.high_comp_officer3_full_na is None
    assert row4.high_comp_officer3_amount is None
    assert row4.high_comp_officer4_full_na is None
    assert row4.high_comp_officer4_amount is None
    assert row4.high_comp_officer5_full_na is None
    assert row4.high_comp_officer5_amount is None
    assert row4.additional_reporting is None
    assert row4.unique_award_key == "CONT_AWD_ABCPIID4_1234_-NONE-_-NONE-"

    assert row5.unique_award_key == "CONT_IDV_CONTPIID1_1234"

    remove_metrics_file()
