from dataactbroker.handlers.fileHandler import fabs_derivations

from tests.unit.dataactcore.factories.domain import (
    CGACFactory, SubTierAgencyFactory, StatesFactory, CountyCodeFactory, CFDAProgramFactory)


def initialize_db_values(db):
    """ Initialize the values in the DB that can be used throughout the tests """
    cgac = CGACFactory()
    db.session.add(cgac)
    db.session.commit()

    cfda_number = CFDAProgramFactory(program_number=12.345)
    sub_tier = SubTierAgencyFactory(sub_tier_agency_code="1234", cgac=cgac)
    state = StatesFactory(state_code="NY")
    county_code = CountyCodeFactory(state_code=state.state_code)
    db.session.add_all([sub_tier, state, county_code, cfda_number])
    db.session.commit()


def initialize_test_obj(fao=None, nffa=None, cfda_num="00.000", sub_tier_code="1234", fund_agency_code=None,
                        sub_fund_agency_code=None, ppop_code="NY00000", ppop_zip4=None, le_zip5=None, record_type=2):
    """ Initialize the values in the object being run through the fabs_derivations function """
    obj = {
        'federal_action_obligation': fao,
        'non_federal_funding_amount': nffa,
        'cfda_number': cfda_num,
        'awarding_sub_tier_agency_c': sub_tier_code,
        'funding_agency_code': fund_agency_code,
        'funding_sub_tier_agency_co': sub_fund_agency_code,
        'place_of_performance_code': ppop_code,
        'place_of_performance_zip4a': ppop_zip4,
        'legal_entity_zip5': le_zip5,
        'record_type': record_type
    }
    return obj


def test_total_funding_amount(database):
    initialize_db_values(database)

    # when fao and nffa are empty
    obj = initialize_test_obj()
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 0

    # when one of fao and nffa is empty and the other isn't
    obj = initialize_test_obj(fao=5.3)
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 5.3

    # when fao and nffa aren't empty
    obj = initialize_test_obj(fao=-10.6, nffa=123)
    obj = fabs_derivations(obj, database.session)
    assert obj['total_funding_amount'] == 112.4
