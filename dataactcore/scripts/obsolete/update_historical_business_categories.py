import logging
import argparse
from datetime import datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.broker_logging import configure_logging
from dataactvalidator.health_check import create_app


logger = logging.getLogger(__name__)

BUSINESS_CATEGORY_FUNCTION_SQL = """
create or replace function compile_fpds_business_categories(
    -- BUSINESS (FOR-PROFIT)
    contracting_officers_deter  text,
    corporate_entity_tax_exemp  boolean,
    corporate_entity_not_tax_e  boolean,
    partnership_or_limited_lia  boolean,
    sole_proprietorship         boolean,
    manufacturer_of_goods       boolean,
    subchapter_s_corporation    boolean,
    limited_liability_corporat  boolean,
    for_profit_organization     boolean,

    -- MINORITY BUSINESS
    alaskan_native_owned_corpo  boolean,
    american_indian_owned_busi  boolean,
    asian_pacific_american_own  boolean,
    black_american_owned_busin  boolean,
    hispanic_american_owned_bu  boolean,
    native_american_owned_busi  boolean,
    native_hawaiian_owned_busi  boolean,
    subcontinent_asian_asian_i  boolean,
    tribally_owned_business     boolean,
    other_minority_owned_busin  boolean,
    minority_owned_business     boolean,

    -- WOMEN OWNED BUSINESS
    women_owned_small_business  boolean,
    economically_disadvantaged  boolean,
    joint_venture_women_owned   boolean,
    joint_venture_economically  boolean,
    woman_owned_business        boolean,

    -- VETERAN OWNED BUSINESS
    service_disabled_veteran_o  boolean,
    veteran_owned_business      boolean,

    -- SPECIAL DESIGNATIONS
    c8a_program_participant     boolean,
    the_ability_one_program     boolean,
    dot_certified_disadvantage  boolean,
    emerging_small_business     boolean,
    federally_funded_research   boolean,
    historically_underutilized  boolean,
    labor_surplus_area_firm     boolean,
    sba_certified_8_a_joint_ve  boolean,
    self_certified_small_disad  boolean,
    small_agricultural_coopera  boolean,
    small_disadvantaged_busine  boolean,
    community_developed_corpor  boolean,
    domestic_or_foreign_entity  text,
    foreign_owned_and_located   boolean,
    foreign_government          boolean,
    international_organization  boolean,
    domestic_shelter            boolean,
    hospital_flag               boolean,
    veterinary_hospital         boolean,

    -- NON-PROFIT
    foundation                  boolean,
    community_development_corp  boolean,
    nonprofit_organization      boolean,

    -- HIGHER EDUCATION
    educational_institution     boolean,
    other_not_for_profit_organ  boolean,
    state_controlled_instituti  boolean,
    c1862_land_grant_college    boolean,
    c1890_land_grant_college    boolean,
    c1994_land_grant_college    boolean,
    private_university_or_coll  boolean,
    minority_institution        boolean,
    historically_black_college  boolean,
    tribal_college              boolean,
    alaskan_native_servicing_i  boolean,
    native_hawaiian_servicing   boolean,
    hispanic_servicing_institu  boolean,
    school_of_forestry          boolean,
    veterinary_college          boolean,

    -- GOVERNMENT
    us_federal_government       boolean,
    federal_agency              boolean,
    us_government_entity        boolean,
    interstate_entity           boolean,
    us_state_government         boolean,
    council_of_governments      boolean,
    city_local_government       boolean,
    county_local_government     boolean,
    inter_municipal_local_gove  boolean,
    municipality_local_governm  boolean,
    township_local_government   boolean,
    us_local_government         boolean,
    local_government_owned      boolean,
    school_district_local_gove  boolean,
    us_tribal_government        boolean,
    indian_tribe_federally_rec  boolean,
    housing_authorities_public  boolean,
    airport_authority           boolean,
    port_authority              boolean,
    transit_authority           boolean,
    planning_commission         boolean
)
returns text[] as $$
declare
    bc_arr text[];
begin

-- BUSINESS (FOR-PROFIT ORGANIZATION)
    if contracting_officers_deter = 'S' or
       women_owned_small_business   IS TRUE or
       economically_disadvantaged   IS TRUE or
       joint_venture_women_owned    IS TRUE or
       emerging_small_business      IS TRUE or
       self_certified_small_disad   IS TRUE or
       small_agricultural_coopera   IS TRUE or
       small_disadvantaged_busine   IS TRUE
    then
        bc_arr := bc_arr || array['small_business'];
    end if;

    if contracting_officers_deter = 'O'
    then
        bc_arr := bc_arr || array['other_than_small_business'];
    end if;

    if corporate_entity_tax_exemp IS TRUE
    then
        bc_arr := bc_arr || array['corporate_entity_tax_exempt'];
    end if;

    if corporate_entity_not_tax_e IS TRUE
    then
        bc_arr := bc_arr || array['corporate_entity_not_tax_exempt'];
    end if;

    if partnership_or_limited_lia IS TRUE
    then
        bc_arr := bc_arr || array['partnership_or_limited_liability_partnership'];
    end if;

    if sole_proprietorship IS TRUE
    then
        bc_arr := bc_arr || array['sole_proprietorship'];
    end if;

    if manufacturer_of_goods IS TRUE
    then
        bc_arr := bc_arr || array['manufacturer_of_goods'];
    end if;

    if subchapter_s_corporation IS TRUE
    then
        bc_arr := bc_arr || array['subchapter_s_corporation'];
    end if;

    if limited_liability_corporat IS TRUE
    then
        bc_arr := bc_arr || array['limited_liability_corporation'];
    end if;

    if for_profit_organization IS TRUE or (bc_arr && array[
        'small_business',
        'other_than_small_business',
        'corporate_entity_tax_exempt',
        'corporate_entity_not_tax_exempt',
        'partnership_or_limited_liability_partnership',
        'sole_proprietorship',
        'manufacturer_of_goods',
        'subchapter_s_corporation',
        'limited_liability_corporation'
    ])
    then
        bc_arr := bc_arr || array['category_business'];
    end if;

-- MINORITY BUSINESS
    if alaskan_native_owned_corpo IS TRUE
    then
        bc_arr := bc_arr || array['alaskan_native_owned_business'];
    end if;

    if american_indian_owned_busi IS TRUE
    then
        bc_arr := bc_arr || array['american_indian_owned_business'];
    end if;

    if asian_pacific_american_own IS TRUE
    then
        bc_arr := bc_arr || array['asian_pacific_american_owned_business'];
    end if;

    if black_american_owned_busin IS TRUE
    then
        bc_arr := bc_arr || array['black_american_owned_business'];
    end if;

    if hispanic_american_owned_bu IS TRUE
    then
        bc_arr := bc_arr || array['hispanic_american_owned_business'];
    end if;

    if native_american_owned_busi IS TRUE
    then
        bc_arr := bc_arr || array['native_american_owned_business'];
    end if;

    if native_hawaiian_owned_busi IS TRUE
    then
        bc_arr := bc_arr || array['native_hawaiian_owned_business'];
    end if;

    if subcontinent_asian_asian_i IS TRUE
    then
        bc_arr := bc_arr || array['subcontinent_asian_indian_american_owned_business'];
    end if;

    if tribally_owned_business IS TRUE
    then
        bc_arr := bc_arr || array['tribally_owned_business'];
    end if;

    if other_minority_owned_busin IS TRUE
    then
        bc_arr := bc_arr || array['other_minority_owned_business'];
    end if;

    if minority_owned_business IS TRUE or (bc_arr && array[
        'alaskan_native_owned_business',
        'american_indian_owned_business',
        'asian_pacific_american_owned_business',
        'black_american_owned_business',
        'hispanic_american_owned_business',
        'native_american_owned_business',
        'native_hawaiian_owned_business',
        'subcontinent_asian_indian_american_owned_business',
        'tribally_owned_business',
        'other_minority_owned_business'
    ])
    then
        bc_arr := bc_arr || array['minority_owned_business'];
    end if;

-- WOMEN OWNED BUSINESS
    if women_owned_small_business IS TRUE
    then
        bc_arr := bc_arr || array['women_owned_small_business'];
    end if;

    if economically_disadvantaged IS TRUE
    then
        bc_arr := bc_arr || array['economically_disadvantaged_women_owned_small_business'];
    end if;

    if joint_venture_women_owned IS TRUE
    then
        bc_arr := bc_arr || array['joint_venture_women_owned_small_business'];
    end if;

    if joint_venture_economically IS TRUE
    then
        bc_arr := bc_arr || array['joint_venture_economically_disadvantaged_women_owned_small_business'];
    end if;

    if woman_owned_business IS TRUE or (bc_arr && array[
        'women_owned_small_business',
        'economically_disadvantaged_women_owned_small_business',
        'joint_venture_women_owned_small_business',
        'joint_venture_economically_disadvantaged_women_owned_small_business'
    ])
    then
        bc_arr := bc_arr || array['woman_owned_business'];
    end if;

-- VETERAN OWNED BUSINESS
    if service_disabled_veteran_o IS TRUE
    then
        bc_arr := bc_arr || array['service_disabled_veteran_owned_business'];
    end if;

    if veteran_owned_business IS TRUE or (bc_arr && array[
        'service_disabled_veteran_owned_business'
    ])
    then
        bc_arr := bc_arr || array['veteran_owned_business'];
    end if;

-- SPECIAL DESIGNATIONS
    if c8a_program_participant IS TRUE
    then
        bc_arr := bc_arr || array['8a_program_participant'];
    end if;

    if the_ability_one_program IS TRUE
    then
        bc_arr := bc_arr || array['ability_one_program'];
    end if;

    if dot_certified_disadvantage IS TRUE
    then
        bc_arr := bc_arr || array['dot_certified_disadvantaged_business_enterprise'];
    end if;

    if emerging_small_business IS TRUE
    then
        bc_arr := bc_arr || array['emerging_small_business'];
    end if;

    if federally_funded_research IS TRUE
    then
        bc_arr := bc_arr || array['federally_funded_research_and_development_corp'];
    end if;

    if historically_underutilized IS TRUE
    then
        bc_arr := bc_arr || array['historically_underutilized_business_firm'];
    end if;

    if labor_surplus_area_firm IS TRUE
    then
        bc_arr := bc_arr || array['labor_surplus_area_firm'];
    end if;

    if sba_certified_8_a_joint_ve IS TRUE
    then
        bc_arr := bc_arr || array['sba_certified_8a_joint_venture'];
    end if;

    if self_certified_small_disad IS TRUE
    then
        bc_arr := bc_arr || array['self_certified_small_disadvanted_business'];
    end if;

    if small_agricultural_coopera IS TRUE
    then
        bc_arr := bc_arr || array['small_agricultural_cooperative'];
    end if;

    if small_disadvantaged_busine IS TRUE
    then
        bc_arr := bc_arr || array['small_disadvantaged_business'];
    end if;

    if community_developed_corpor IS TRUE
    then
        bc_arr := bc_arr || array['community_developed_corporation_owned_firm'];
    end if;

    -- U.S. Owned Business
    if domestic_or_foreign_entity = 'A'
    then
        bc_arr := bc_arr || array['us_owned_business'];
    end if;

    -- Foreign-Owned Business Incorporated in the U.S.
    if domestic_or_foreign_entity = 'C'
    then
        bc_arr := bc_arr || array['foreign_owned_and_us_located_business'];
    end if;

    -- Foreign-Owned Business Not Incorporated in the U.S.
    if domestic_or_foreign_entity = 'D' or foreign_owned_and_located IS TRUE
    then
        bc_arr := bc_arr || array['foreign_owned_and_located_business'];
    end if;

    if foreign_government IS TRUE
    then
        bc_arr := bc_arr || array['foreign_government'];
    end if;

    if international_organization IS TRUE
    then
        bc_arr := bc_arr || array['international_organization'];
    end if;

    if domestic_shelter IS TRUE
    then
        bc_arr := bc_arr || array['domestic_shelter'];
    end if;

    if hospital_flag IS TRUE
    then
        bc_arr := bc_arr || array['hospital'];
    end if;

    if veterinary_hospital IS TRUE
    then
        bc_arr := bc_arr || array['veterinary_hospital'];
    end if;

    if bc_arr && array[
        '8a_program_participant',
        'ability_one_program',
        'dot_certified_disadvantaged_business_enterprise',
        'emerging_small_business',
        'federally_funded_research_and_development_corp',
        'historically_underutilized_business_firm',
        'labor_surplus_area_firm',
        'sba_certified_8a_joint_venture',
        'self_certified_small_disadvanted_business',
        'small_agricultural_cooperative',
        'small_disadvantaged_business',
        'community_developed_corporation_owned_firm',
        'us_owned_business',
        'foreign_owned_and_us_located_business',
        'foreign_owned_and_located_business',
        'foreign_government',
        'international_organization',
        'domestic_shelter',
        'hospital',
        'veterinary_hospital'
     ]
    then
        bc_arr := bc_arr || array['special_designations'];
    end if;

-- NON-PROFIT
    if foundation IS TRUE
    then
        bc_arr := bc_arr || array['foundation'];
    end if;

    if community_development_corp IS TRUE
    then
        bc_arr := bc_arr || array['community_development_corporations'];
    end if;

    if nonprofit_organization       IS TRUE or
       other_not_for_profit_organ   IS TRUE or
       (bc_arr && array[
            'foundation',
            'community_development_corporations'
       ])
    then
        bc_arr := bc_arr || array['nonprofit'];
    end if;

-- HIGHER EDUCATION
    if educational_institution IS TRUE
    then
        bc_arr := bc_arr || array['educational_institution'];
    end if;

    if state_controlled_instituti   IS TRUE or
       c1862_land_grant_college     IS TRUE or
       c1890_land_grant_college     IS TRUE or
       c1994_land_grant_college     IS TRUE
    then
        bc_arr := bc_arr || array['public_institution_of_higher_education'];
    end if;

    if private_university_or_coll IS TRUE
    then
        bc_arr := bc_arr || array['private_institution_of_higher_education'];
    end if;

    if minority_institution         IS TRUE or
       historically_black_college   IS TRUE or
       tribal_college               IS TRUE or
       alaskan_native_servicing_i   IS TRUE or
       native_hawaiian_servicing    IS TRUE or
       hispanic_servicing_institu   IS TRUE
    then
        bc_arr := bc_arr || array['minority_serving_institution_of_higher_education'];
    end if;

    if school_of_forestry IS TRUE
    then
        bc_arr := bc_arr || array['school_of_forestry'];
    end if;

    if veterinary_college IS TRUE
    then
        bc_arr := bc_arr || array['veterinary_college'];
    end if;

    if bc_arr && array[
        'educational_institution',
        'public_institution_of_higher_education',
        'private_institution_of_higher_education',
        'minority_serving_institution_of_higher_education',
        'school_of_forestry',
        'veterinary_college'
    ]
    then
        bc_arr := bc_arr || array['higher_education'];
    end if;

-- GOVERNMENT
    if us_federal_government    IS TRUE or
       federal_agency           IS TRUE or
       us_government_entity     IS TRUE
    then
        bc_arr := bc_arr || array['national_government'];
    end if;

    if interstate_entity IS TRUE
    then
        bc_arr := bc_arr || array['interstate_entity'];
    end if;

    if us_state_government IS TRUE
    then
        bc_arr := bc_arr || array['regional_and_state_government'];
    end if;

    if council_of_governments IS TRUE
    then
        bc_arr := bc_arr || array['council_of_governments'];
    end if;

    if city_local_government        IS TRUE or
       county_local_government      IS TRUE or
       inter_municipal_local_gove   IS TRUE or
       municipality_local_governm   IS TRUE or
       township_local_government    IS TRUE or
       us_local_government          IS TRUE or
       local_government_owned       IS TRUE or
       school_district_local_gove   IS TRUE
    then
        bc_arr := bc_arr || array['local_government'];
    end if;

    if us_tribal_government         IS TRUE or
       indian_tribe_federally_rec   IS TRUE
    then
        bc_arr := bc_arr || array['indian_native_american_tribal_government'];
    end if;

    if housing_authorities_public   IS TRUE or
       airport_authority            IS TRUE or
       port_authority               IS TRUE or
       transit_authority            IS TRUE or
       planning_commission          IS TRUE
    then
        bc_arr := bc_arr || array['authorities_and_commissions'];
    end if;

    if bc_arr && array[
        'national_government',
        'regional_and_state_government',
        'local_government',
        'indian_native_american_tribal_government',
        'authorities_and_commissions',
        'interstate_entity',
        'council_of_governments'
    ]
    then
        bc_arr := bc_arr || array['government'];
    end if;

    return bc_arr;
end;
$$  language plpgsql;

create or replace function compile_fabs_business_categories(business_types text)
returns text[]
immutable parallel safe
as $$
declare
    bc_arr text[];
begin

-- BUSINESS (FOR-PROFIT ORGANIZATION)
    if business_types ~ '(R|23)'
    then
        bc_arr := bc_arr || array['small_business'];
    end if;

    if business_types ~ '(Q|22)'
    then
        bc_arr := bc_arr || array['other_than_small_business'];
    end if;

    if bc_arr && array['small_business', 'other_than_small_business']
    then
        bc_arr := bc_arr || array['category_business'];
    end if;

-- NON-PROFIT
    if business_types ~ '(M|N|12)'
    then
        bc_arr := bc_arr || array['nonprofit'];
    end if;

-- HIGHER EDUCATION
    if business_types ~ '(H|06)'
    then
        bc_arr := bc_arr || array['public_institution_of_higher_education'];
    end if;

    if business_types ~ '(O|20)'
    then
        bc_arr := bc_arr || array['private_institution_of_higher_education'];
    end if;

    if business_types ~ '(T|U|V|S)'
    then
        bc_arr := bc_arr || array['minority_serving_institution_of_higher_education'];
    end if;

    if bc_arr && array[
        'public_institution_of_higher_education',
        'private_institution_of_higher_education',
        'minority_serving_institution_of_higher_education'
    ]
    then
        bc_arr := bc_arr || array['higher_education'];
    end if;

-- GOVERNMENT
    if business_types ~ '(A|00)'
    then
        bc_arr := bc_arr || array['regional_and_state_government'];
    end if;

    if business_types ~ '(E)'
    then
        bc_arr := bc_arr || array['regional_organization'];
    end if;

    if business_types ~ '(F)'
    then
        bc_arr := bc_arr || array['us_territory_or_possession'];
    end if;

    if business_types ~ '(B|C|D|G|01|02|04|05)'
    then
        bc_arr := bc_arr || array['local_government'];
    end if;

    if business_types ~ '(I|J|K|11)'
    then
        bc_arr := bc_arr || array['indian_native_american_tribal_government'];
    end if;

    if business_types ~ '(L)'
    then
        bc_arr := bc_arr || array['authorities_and_commissions'];
    end if;

    if bc_arr && array[
        'regional_and_state_government',
        'us_territory_or_possession',
        'local_government',
        'indian_native_american_tribal_government',
        'authorities_and_commissions',
        'regional_organization'
    ]
    then
        bc_arr := bc_arr || array['government'];
    end if;

-- INDIVIDUALS
    if business_types ~ '(P|21)'
    then
        bc_arr := bc_arr || array['individuals'];
    end if;

    -- Sort and return the array.
    return array(select unnest(bc_arr) order by 1);
end;
$$  language plpgsql;
"""

FPDS_UPDATE_SQL = """
    UPDATE detached_award_procurement
    SET business_categories = compile_fpds_business_categories(
        contracting_officers_deter,
        corporate_entity_tax_exemp::BOOLEAN,
        corporate_entity_not_tax_e::BOOLEAN,
        partnership_or_limited_lia::BOOLEAN,
        sole_proprietorship::BOOLEAN,
        manufacturer_of_goods::BOOLEAN,
        subchapter_s_corporation::BOOLEAN,
        limited_liability_corporat::BOOLEAN,
        for_profit_organization::BOOLEAN,
        alaskan_native_owned_corpo::BOOLEAN,
        american_indian_owned_busi::BOOLEAN,
        asian_pacific_american_own::BOOLEAN,
        black_american_owned_busin::BOOLEAN,
        hispanic_american_owned_bu::BOOLEAN,
        native_american_owned_busi::BOOLEAN,
        native_hawaiian_owned_busi::BOOLEAN,
        subcontinent_asian_asian_i::BOOLEAN,
        tribally_owned_business::BOOLEAN,
        other_minority_owned_busin::BOOLEAN,
        minority_owned_business::BOOLEAN,
        women_owned_small_business::BOOLEAN,
        economically_disadvantaged::BOOLEAN,
        joint_venture_women_owned::BOOLEAN,
        joint_venture_economically::BOOLEAN,
        woman_owned_business::BOOLEAN,
        service_disabled_veteran_o::BOOLEAN,
        veteran_owned_business::BOOLEAN,
        c8a_program_participant::BOOLEAN,
        the_ability_one_program::BOOLEAN,
        dot_certified_disadvantage::BOOLEAN,
        emerging_small_business::BOOLEAN,
        federally_funded_research::BOOLEAN,
        historically_underutilized::BOOLEAN,
        labor_surplus_area_firm::BOOLEAN,
        sba_certified_8_a_joint_ve::BOOLEAN,
        self_certified_small_disad::BOOLEAN,
        small_agricultural_coopera::BOOLEAN,
        small_disadvantaged_busine::BOOLEAN,
        community_developed_corpor::BOOLEAN,
        domestic_or_foreign_entity,
        foreign_owned_and_located::BOOLEAN,
        foreign_government::BOOLEAN,
        international_organization::BOOLEAN,
        domestic_shelter::BOOLEAN,
        hospital_flag::BOOLEAN,
        veterinary_hospital::BOOLEAN,
        foundation::BOOLEAN,
        community_development_corp::BOOLEAN,
        nonprofit_organization::BOOLEAN,
        educational_institution::BOOLEAN,
        other_not_for_profit_organ::BOOLEAN,
        state_controlled_instituti::BOOLEAN,
        c1862_land_grant_college::BOOLEAN,
        c1890_land_grant_college::BOOLEAN,
        c1994_land_grant_college::BOOLEAN,
        private_university_or_coll::BOOLEAN,
        minority_institution::BOOLEAN,
        historically_black_college::BOOLEAN,
        tribal_college::BOOLEAN,
        alaskan_native_servicing_i::BOOLEAN,
        native_hawaiian_servicing::BOOLEAN,
        hispanic_servicing_institu::BOOLEAN,
        school_of_forestry::BOOLEAN,
        veterinary_college::BOOLEAN,
        us_federal_government::BOOLEAN,
        federal_agency::BOOLEAN,
        us_government_entity::BOOLEAN,
        interstate_entity::BOOLEAN,
        us_state_government::BOOLEAN,
        council_of_governments::BOOLEAN,
        city_local_government::BOOLEAN,
        county_local_government::BOOLEAN,
        inter_municipal_local_gove::BOOLEAN,
        municipality_local_governm::BOOLEAN,
        township_local_government::BOOLEAN,
        us_local_government::BOOLEAN,
        local_government_owned::BOOLEAN,
        school_district_local_gove::BOOLEAN,
        us_tribal_government::BOOLEAN,
        indian_tribe_federally_rec::BOOLEAN,
        housing_authorities_public::BOOLEAN,
        airport_authority::BOOLEAN,
        port_authority::BOOLEAN,
        transit_authority::BOOLEAN,
        planning_commission::BOOLEAN
    )
    {};
"""

FABS_UPDATE_SQL = """
    UPDATE published_fabs
    SET business_categories = compile_fabs_business_categories(UPPER(business_types))
    WHERE is_active IS TRUE{};
"""


def update_fpds_business_categories(sess, update_empty=False):
    start = datetime.now()
    logger.info("Updating business categories for FPDS")
    empty_sql = " WHERE business_categories = '{}'" if update_empty else ""
    result = sess.execute(FPDS_UPDATE_SQL.format(empty_sql))
    logger.info(
        "Finished updating business categories for FDPS in %s seconds (%s rows updated)"
        % (str(datetime.now() - start), result.rowcount)
    )


def update_fabs_business_categories(sess, update_empty=False):
    start = datetime.now()
    logger.info("Updating business categories for FABS")
    empty_sql = " AND business_categories = '{}'" if update_empty else ""
    result = sess.execute(FABS_UPDATE_SQL.format(empty_sql))
    logger.info(
        "Finished updating business categories for FABS in %s seconds (%s rows updated)"
        % (str(datetime.now() - start), result.rowcount)
    )


def main():
    sess = GlobalDB.db().session

    parser = argparse.ArgumentParser(description="Update business categories for existing transaction data")
    parser.add_argument("-fpds", help="Update only FPDS business categories", action="store_true")
    parser.add_argument("-fabs", help="Update only FABS business categories", action="store_true")
    parser.add_argument("-update_empty", help="Only update ones without categories", action="store_true")
    args = parser.parse_args()

    overall_start = datetime.now()
    logger.info("Starting business categories updates")

    start = datetime.now()
    logger.info("Recreating business category SQL functions")
    sess.execute(BUSINESS_CATEGORY_FUNCTION_SQL)
    sess.commit()
    logger.info("Finished recreating business category SQL functions in %s seconds" % str(datetime.now() - start))

    if (args.fpds and args.fabs) or not (args.fpds or args.fabs):
        update_fpds_business_categories(sess, args.update_empty)
        update_fabs_business_categories(sess, args.update_empty)
    elif args.fpds:
        update_fpds_business_categories(sess, args.update_empty)
    elif args.fabs:
        update_fabs_business_categories(sess, args.update_empty)
    sess.commit()

    logger.info("Completed business categories updates in %s seconds" % str(datetime.now() - overall_start))


if __name__ == "__main__":
    with create_app().app_context():
        configure_logging()
        main()
