import re
import logging

from datetime import datetime
from sqlalchemy import func

from dataactcore.models.domainModels import (CFDAProgram, SubTierAgency, Zips, States, CountyCode, CityCode, ZipCity,
                                             CountryCode)
from dataactcore.models.stagingModels import FPDSContractingOffice

logger = logging.getLogger(__name__)


# TODO: Make these lookups (potentially) instead of DB calls, ordered from smallest to largest: States (60 entries),
# CountryCode (264 entries), SubTierAgency (1,476 entries), CFDAProgram (2,971 entries), CountyCode (3,295 entries),
# FPDSContractingOffice (6,566 entries)

def get_zip_data(sess, zip_five, zip_four):
    """ Get zip data based on 5-digit or 9-digit zips and the counts of congressional districts associated with them """
    zip_info = None
    cd_count = 1

    # if we have a 4-digit zip to work with, try using both
    if zip_four:
        zip_info = sess.query(Zips).filter_by(zip5=zip_five, zip_last4=zip_four).first()
    # if we didn't manage to find anything using 9 digits or we don't have 9 digits, try to find one using just 5 digits
    if not zip_info:
        zip_info = sess.query(Zips).filter_by(zip5=zip_five).first()
        # if this is a 5-digit zip, there may be more than one congressional district associated with it
        cd_count = sess.query(Zips.congressional_district_no.label('cd_count')). \
            filter_by(zip5=zip_five).distinct().count()

    return zip_info, cd_count


def derive_cfda(obj, sess, job_id, detached_award_financial_assistance_id):
    """ Deriving cfda title from cfda number using cfda program table """
    cfda_title = sess.query(CFDAProgram).filter_by(program_number=obj['cfda_number']).one_or_none()
    if cfda_title:
        obj['cfda_title'] = cfda_title.program_title
    else:
        logger.error({
            'message': 'CFDA title not found for CFDA number {}'.format(obj['cfda_number']),
            'message_type': 'BrokerError',
            'job_id': job_id,
            'detached_award_financial_assistance_id': detached_award_financial_assistance_id
        })
        obj['cfda_title'] = None


def derive_awarding_agency_data(obj, sess):
    """ Deriving awarding sub tier agency name, awarding agency name, and awarding agency code """
    if obj['awarding_sub_tier_agency_c']:
        awarding_sub_tier = sess.query(SubTierAgency).\
            filter_by(sub_tier_agency_code=obj['awarding_sub_tier_agency_c']).one()
        use_frec = awarding_sub_tier.is_frec
        awarding_agency = awarding_sub_tier.frec if use_frec else awarding_sub_tier.cgac
        obj['awarding_agency_code'] = awarding_agency.frec_code if use_frec else awarding_agency.cgac_code
        obj['awarding_agency_name'] = awarding_agency.agency_name
        obj['awarding_sub_tier_agency_n'] = awarding_sub_tier.sub_tier_agency_name
    else:
        obj['awarding_agency_code'] = None
        obj['awarding_agency_name'] = None
        obj['awarding_sub_tier_agency_n'] = None


def derive_funding_agency_data(obj, sess):
    """ Deriving funding sub tier agency name, funding agency name, and funding agency code """
    if obj['funding_sub_tier_agency_co']:
        funding_sub_tier_agency = sess.query(SubTierAgency). \
            filter_by(sub_tier_agency_code=obj['funding_sub_tier_agency_co']).one()
        obj['funding_sub_tier_agency_na'] = funding_sub_tier_agency.sub_tier_agency_name
        use_frec = funding_sub_tier_agency.is_frec
        funding_agency = funding_sub_tier_agency.frec if use_frec else funding_sub_tier_agency.cgac
        obj['funding_agency_code'] = funding_agency.frec_code if use_frec else funding_agency.cgac_code
        obj['funding_agency_name'] = funding_agency.agency_name
        obj['funding_sub_tier_agency_na'] = funding_sub_tier_agency.sub_tier_agency_name
    else:
        obj['funding_sub_tier_agency_na'] = None
        obj['funding_agency_name'] = None
        obj['funding_agency_code'] = None


def derive_ppop_state(obj, sess):
    """ Deriving ppop code and ppop state name """
    # deriving ppop state name
    ppop_code = None
    ppop_state = None
    if obj['place_of_performance_code']:
        ppop_code = obj['place_of_performance_code'].upper()
        if ppop_code == '00*****':
            ppop_state = States(state_code=None, state_name='Multi-state')
        elif ppop_code == '00FORGN':
            ppop_state = States(state_code=None, state_name=None)
        else:
            ppop_state = sess.query(States).filter_by(state_code=ppop_code[:2]).one()
        obj['place_of_perfor_state_code'] = ppop_state.state_code
        obj['place_of_perform_state_nam'] = ppop_state.state_name
    else:
        obj['place_of_perfor_state_code'] = None
        obj['place_of_perform_state_nam'] = None

    return ppop_code, ppop_state


def derive_ppop_location_data(obj, sess, ppop_code, ppop_state):
    """ Deriving place of performance location values from zip4 """
    if obj['place_of_performance_zip4a'] and obj['place_of_performance_zip4a'] != 'city-wide':
        zip_five = obj['place_of_performance_zip4a'][:5]
        zip_four = None

        # if zip4 is 9 digits, set the zip_four value to the last 4 digits
        if len(obj['place_of_performance_zip4a']) > 5:
            zip_four = obj['place_of_performance_zip4a'][-4:]

        zip_info, cd_count = get_zip_data(sess, zip_five, zip_four)

        # deriving ppop congressional district
        if not obj['place_of_performance_congr']:
            if zip_info.congressional_district_no and cd_count == 1:
                obj['place_of_performance_congr'] = zip_info.congressional_district_no
            else:
                obj['place_of_performance_congr'] = '90'

        # deriving PrimaryPlaceOfPerformanceCountyName/Code
        obj['place_of_perform_county_co'] = zip_info.county_number
        county_info = sess.query(CountyCode). \
            filter_by(county_number=zip_info.county_number, state_code=zip_info.state_abbreviation).first()
        if county_info:
            obj['place_of_perform_county_na'] = county_info.county_name
        else:
            obj['place_of_perform_county_na'] = None

        # deriving PrimaryPlaceOfPerformanceCityName
        city_info = sess.query(ZipCity).filter_by(zip_code=zip_five).one()
        obj['place_of_performance_city'] = city_info.city_name
    # if there is no ppop zip4, we need to try to derive county/city info from the ppop code
    elif ppop_code:
        # if ppop_code is in county format,
        if re.match('^[A-Z]{2}\*\*\d{3}$', ppop_code):
            # getting county name
            county_code = ppop_code[-3:]
            county_info = sess.query(CountyCode). \
                filter_by(county_number=county_code, state_code=ppop_state.state_code).first()
            obj['place_of_perform_county_co'] = county_code
            obj['place_of_perform_county_na'] = county_info.county_name
            obj['place_of_performance_city'] = None
        # if ppop_code is in city format
        elif re.match('^[A-Z]{2}\d{5}$', ppop_code) and not re.match('^[A-Z]{2}0{5}$', ppop_code):
            # getting city and county name
            city_code = ppop_code[-5:]
            city_info = sess.query(CityCode).filter_by(city_code=city_code, state_code=ppop_state.state_code).first()
            obj['place_of_performance_city'] = city_info.feature_name
            obj['place_of_perform_county_co'] = city_info.county_number
            obj['place_of_perform_county_na'] = city_info.county_name
    # if there's no ppop code, just set them all to None
    else:
        obj['place_of_perform_county_co'] = None
        obj['place_of_perform_county_na'] = None
        obj['place_of_performance_city'] = None


def derive_le_location_data(obj, sess, ppop_code, ppop_state):
    """ Deriving place of performance location values """
    # Deriving from zip code (record type is 2 or 3 in this case)
    if obj['legal_entity_zip5']:
        # legal entity city data
        city_info = sess.query(ZipCity).filter_by(zip_code=obj['legal_entity_zip5']).one()
        obj['legal_entity_city_name'] = city_info.city_name

        zip_data, cd_count = get_zip_data(sess, obj['legal_entity_zip5'], obj['legal_entity_zip_last4'])

        # deriving legal entity congressional district
        if not obj['legal_entity_congressional']:
            if zip_data.congressional_district_no and cd_count == 1:
                obj['legal_entity_congressional'] = zip_data.congressional_district_no
            else:
                obj['legal_entity_congressional'] = '90'

        # legal entity city data
        county_info = sess.query(CountyCode). \
            filter_by(county_number=zip_data.county_number, state_code=zip_data.state_abbreviation).first()
        if county_info:
            obj['legal_entity_county_code'] = county_info.county_number
            obj['legal_entity_county_name'] = county_info.county_name
        else:
            obj['legal_entity_county_code'] = None
            obj['legal_entity_county_name'] = None

        # legal entity state data
        state_info = sess.query(States).filter_by(state_code=zip_data.state_abbreviation).one()
        obj['legal_entity_state_code'] = state_info.state_code
        obj['legal_entity_state_name'] = state_info.state_name

    # deriving legal entity stuff that's based on record type of 1
    # (ppop code must be in the format XX**###, XX*****, 00FORGN for these)
    if obj['record_type'] == 1:

        county_wide_pattern = re.compile("^[a-zA-Z]{2}\*{2}\d{3}$")
        state_wide_pattern = re.compile("^[a-zA-Z]{2}\*{5}$")

        obj['legal_entity_county_code'] = None
        obj['legal_entity_county_name'] = None
        obj['legal_entity_state_code'] = None
        obj['legal_entity_state_name'] = None
        obj['legal_entity_congressional'] = None

        if county_wide_pattern.match(ppop_code):
            # legal entity county data
            county_code = ppop_code[-3:]
            county_info = sess.query(CountyCode). \
                filter_by(county_number=county_code, state_code=ppop_state.state_code).first()
            obj['legal_entity_county_code'] = county_code
            obj['legal_entity_county_name'] = county_info.county_name

        if county_wide_pattern.match(ppop_code) or state_wide_pattern.match(ppop_code):
            # legal entity state data
            obj['legal_entity_state_code'] = ppop_state.state_code
            obj['legal_entity_state_name'] = ppop_state.state_name

        # legal entity cd data
        if not obj['legal_entity_congressional'] and county_wide_pattern.match(ppop_code):
            obj['legal_entity_congressional'] = obj['place_of_performance_congr']


def derive_awarding_office_name(obj, sess):
    """ Deriving awarding_office_name based off awarding_office_code """
    if obj['awarding_office_code']:
        award_office = sess.query(FPDSContractingOffice). \
            filter_by(contracting_office_code=obj['awarding_office_code']).one_or_none()
        if award_office:
            obj['awarding_office_name'] = award_office.contracting_office_name
        else:
            obj['awarding_office_name'] = None


def derive_funding_office_name(obj, sess):
    """ Deriving funding_office_name based off funding_office_code """
    if obj['funding_office_code']:
        funding_office = sess.query(FPDSContractingOffice). \
            filter_by(contracting_office_code=func.upper(obj['funding_office_code'])).one_or_none()
        if funding_office:
            obj['funding_office_name'] = funding_office.contracting_office_name
        else:
            obj['funding_office_name'] = None


def derive_le_city_code(obj, sess):
    """ Deriving legal entity city code """
    if obj['legal_entity_city_name'] and obj['legal_entity_state_code']:
        city_code = sess.query(CityCode).\
            filter(func.lower(CityCode.feature_name) == func.lower(obj['legal_entity_city_name'].strip()),
                   func.lower(CityCode.state_code) == func.lower(obj['legal_entity_state_code'].strip())).first()
        if city_code:
            obj['legal_entity_city_code'] = city_code.city_code
        else:
            obj['legal_entity_city_code'] = None


def derive_ppop_country_name(obj, sess):
    """ Deriving place of performance country name """
    if obj['place_of_perform_country_c']:
        country_data = sess.query(CountryCode). \
            filter_by(country_code=obj['place_of_perform_country_c'].upper()).one_or_none()
        if country_data:
            obj['place_of_perform_country_n'] = country_data.country_name
        else:
            obj['place_of_perform_country_n'] = None


def derive_le_country_name(obj, sess):
    """ Deriving legal entity country name """
    if obj['legal_entity_country_code']:
        country_data = sess.query(CountryCode). \
            filter_by(country_code=obj['legal_entity_country_code'].upper()).one_or_none()
        if country_data:
            obj['legal_entity_country_name'] = country_data.country_name
        else:
            obj['legal_entity_country_name'] = None


def split_ppop_zip(obj):
    """ Splitting ppop zip code into 5 and 4 digit codes for ease of website access """
    if obj['place_of_performance_zip4a'] and re.match('^\d{5}(-?\d{4})?$', obj['place_of_performance_zip4a']):
        if len(obj['place_of_performance_zip4a']) == 5:
            obj['place_of_performance_zip5'] = obj['place_of_performance_zip4a'][:5]
            obj['place_of_perform_zip_last4'] = None
        else:
            obj['place_of_performance_zip5'] = obj['place_of_performance_zip4a'][:5]
            obj['place_of_perform_zip_last4'] = obj['place_of_performance_zip4a'][-4:]


def derive_parent_duns(obj):
    """ Deriving parent DUNS name and number from SAMS API"""
    

def set_active(obj):
    """ Setting active  """
    if obj['correction_delete_indicatr'] and obj['correction_delete_indicatr'].upper() == 'D':
        obj['is_active'] = False
    else:
        obj['is_active'] = True


def fabs_derivations(obj, sess):
    # copy log data and remove keys in the row left for logging
    job_id = obj['job_id']
    detached_award_financial_assistance_id = obj['detached_award_financial_assistance_id']
    obj.pop('detached_award_financial_assistance_id', None)
    obj.pop('job_id', None)

    # initializing a few of the derivations so the keys exist
    obj['legal_entity_state_code'] = None
    obj['legal_entity_city_name'] = None
    obj['place_of_performance_zip5'] = None
    obj['place_of_perform_zip_last4'] = None
    obj['ultimate_parent_legal_enti'] = None
    obj['ultimate_parent_unique_ide'] = None

    # deriving total_funding_amount
    federal_action_obligation = obj['federal_action_obligation'] or 0
    non_federal_funding_amount = obj['non_federal_funding_amount'] or 0
    obj['total_funding_amount'] = federal_action_obligation + non_federal_funding_amount

    derive_cfda(obj, sess, job_id, detached_award_financial_assistance_id)

    derive_awarding_agency_data(obj, sess)

    derive_funding_agency_data(obj, sess)

    ppop_code, ppop_state = derive_ppop_state(obj, sess)

    derive_ppop_location_data(obj, sess, ppop_code, ppop_state)

    derive_le_location_data(obj, sess, ppop_code, ppop_state)

    derive_awarding_office_name(obj, sess)

    derive_funding_office_name(obj, sess)

    derive_le_city_code(obj, sess)

    derive_ppop_country_name(obj, sess)

    derive_le_country_name(obj, sess)

    split_ppop_zip(obj)

    derive_parent_duns(obj)

    set_active(obj)

    obj['modified_at'] = datetime.utcnow()

    return obj
