import re
import logging

from datetime import datetime
from sqlalchemy import func

from dataactcore.models.domainModels import Zips, CityCode, ZipCity, DUNS
from dataactcore.models.lookups import (ACTION_TYPE_DICT, ASSISTANCE_TYPE_DICT, CORRECTION_DELETE_IND_DICT,
                                        RECORD_TYPE_DICT, BUSINESS_TYPE_DICT, BUSINESS_FUNDS_IND_DICT)
from dataactcore.utils.business_categories import get_business_categories

logger = logging.getLogger(__name__)


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


def derive_cfda(obj, cfda_dict, job_id, detached_award_financial_assistance_id):
    """ Deriving cfda title from cfda number using cfda program table """
    obj['cfda_title'] = cfda_dict.get(obj['cfda_number'])
    if not obj['cfda_title']:
        logger.error({
            'message': 'CFDA title not found for CFDA number {}'.format(obj['cfda_number']),
            'message_type': 'BrokerError',
            'job_id': job_id,
            'detached_award_financial_assistance_id': detached_award_financial_assistance_id
        })


def derive_awarding_agency_data(obj, sub_tier_dict):
    """ Deriving awarding sub tier agency name, awarding agency name, and awarding agency code """
    if obj['awarding_sub_tier_agency_c']:
        sub_tier = sub_tier_dict.get(obj['awarding_sub_tier_agency_c'])
        use_frec = sub_tier["is_frec"]
        obj['awarding_agency_code'] = sub_tier["frec_code"] if use_frec else sub_tier["cgac_code"]
        obj['awarding_agency_name'] = sub_tier["agency_name"]
        obj['awarding_sub_tier_agency_n'] = sub_tier["sub_tier_agency_name"]
    else:
        obj['awarding_agency_code'] = None
        obj['awarding_agency_name'] = None
        obj['awarding_sub_tier_agency_n'] = None


def derive_funding_agency_data(obj, sub_tier_dict):
    """ Deriving funding sub tier agency name, funding agency name, and funding agency code """
    if obj['funding_sub_tier_agency_co']:
        sub_tier = sub_tier_dict.get(obj['funding_sub_tier_agency_co'])
        use_frec = sub_tier["is_frec"]
        obj['funding_agency_code'] = sub_tier["frec_code"] if use_frec else sub_tier["cgac_code"]
        obj['funding_agency_name'] = sub_tier["agency_name"]
        obj['funding_sub_tier_agency_na'] = sub_tier["sub_tier_agency_name"]
    else:
        obj['funding_sub_tier_agency_na'] = None
        obj['funding_agency_name'] = None
        obj['funding_agency_code'] = None


def derive_ppop_state(obj, state_dict):
    """ Deriving ppop code and ppop state name """
    # deriving ppop state name
    ppop_code = None
    state_code = None
    state_name = None
    if obj['place_of_performance_code']:
        ppop_code = obj['place_of_performance_code'].upper()
        if ppop_code == '00*****':
            state_name = 'Multi-state'
        elif ppop_code != '00FORGN':
            state_code = ppop_code[:2]
            state_name = state_dict.get(state_code)

    obj['place_of_perfor_state_code'] = state_code
    obj['place_of_perform_state_nam'] = state_name

    return ppop_code, state_code, state_name


def derive_ppop_location_data(obj, sess, ppop_code, ppop_state_code, county_dict):
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
        obj['place_of_perform_county_na'] = county_dict.get(zip_info.state_abbreviation + zip_info.county_number)

        # deriving PrimaryPlaceOfPerformanceCityName
        city_info = sess.query(ZipCity).filter_by(zip_code=zip_five).one()
        obj['place_of_performance_city'] = city_info.city_name
    # if there is no ppop zip4, we need to try to derive county/city info from the ppop code
    elif ppop_code:
        # if ppop_code is in county format,
        if re.match('^[A-Z]{2}\*\*\d{3}$', ppop_code):
            # getting county name
            county_code = ppop_code[-3:]
            obj['place_of_perform_county_co'] = county_code
            obj['place_of_perform_county_na'] = county_dict.get(ppop_state_code + county_code)
            obj['place_of_performance_city'] = None
        # if ppop_code is in city format
        elif re.match('^[A-Z]{2}\d{5}$', ppop_code) and not re.match('^[A-Z]{2}0{5}$', ppop_code):
            # getting city and county name
            city_code = ppop_code[-5:]
            city_info = sess.query(CityCode).filter_by(city_code=city_code, state_code=ppop_state_code).first()
            obj['place_of_performance_city'] = city_info.feature_name
            obj['place_of_perform_county_co'] = city_info.county_number
            obj['place_of_perform_county_na'] = city_info.county_name
    # if there's no ppop code, just set them all to None
    else:
        obj['place_of_perform_county_co'] = None
        obj['place_of_perform_county_na'] = None
        obj['place_of_performance_city'] = None


def derive_le_location_data(obj, sess, ppop_code, state_dict, ppop_state_code, ppop_state_name, county_dict):
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

        # legal entity county data
        obj['legal_entity_county_code'] = zip_data.county_number
        obj['legal_entity_county_name'] = county_dict.get(zip_data.state_abbreviation + zip_data.county_number)

        # legal entity state data
        obj['legal_entity_state_code'] = zip_data.state_abbreviation
        obj['legal_entity_state_name'] = state_dict.get(zip_data.state_abbreviation.upper())

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
            obj['legal_entity_county_code'] = county_code
            obj['legal_entity_county_name'] = county_dict.get(ppop_state_code + county_code)

        if county_wide_pattern.match(ppop_code) or state_wide_pattern.match(ppop_code):
            # legal entity state data
            obj['legal_entity_state_code'] = ppop_state_code
            obj['legal_entity_state_name'] = ppop_state_name

        # legal entity cd data
        if not obj['legal_entity_congressional'] and county_wide_pattern.match(ppop_code):
            obj['legal_entity_congressional'] = obj['place_of_performance_congr']


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


def derive_ppop_country_name(obj, country_dict):
    """ Deriving place of performance country name """
    if obj['place_of_perform_country_c']:
        obj['place_of_perform_country_n'] = country_dict.get(obj['place_of_perform_country_c'].upper())


def derive_le_country_name(obj, country_dict):
    """ Deriving legal entity country name """
    if obj['legal_entity_country_code']:
        obj['legal_entity_country_name'] = country_dict.get(obj['legal_entity_country_code'].upper())


def split_ppop_zip(obj):
    """ Splitting ppop zip code into 5 and 4 digit codes for ease of website access """
    if obj['place_of_performance_zip4a'] and re.match('^\d{5}(-?\d{4})?$', obj['place_of_performance_zip4a']):
        if len(obj['place_of_performance_zip4a']) == 5:
            obj['place_of_performance_zip5'] = obj['place_of_performance_zip4a'][:5]
            obj['place_of_perform_zip_last4'] = None
        else:
            obj['place_of_performance_zip5'] = obj['place_of_performance_zip4a'][:5]
            obj['place_of_perform_zip_last4'] = obj['place_of_performance_zip4a'][-4:]


def derive_parent_duns(obj, sess):
    """ Deriving parent DUNS name and number from SAMS API"""
    if obj['awardee_or_recipient_uniqu']:
        duns_data = sess.query(DUNS).\
            filter_by(awardee_or_recipient_uniqu=obj['awardee_or_recipient_uniqu']).one_or_none()
        if duns_data:
            obj['ultimate_parent_legal_enti'] = duns_data.ultimate_parent_legal_enti
            obj['ultimate_parent_unique_ide'] = duns_data.ultimate_parent_unique_ide
        else:
            obj['ultimate_parent_legal_enti'] = None
            obj['ultimate_parent_unique_ide'] = None


def derive_labels(obj):
    """ Deriving labels for codes entered by the user """

    # Derive description for ActionType
    if obj['action_type']:
        obj['action_type_description'] = ACTION_TYPE_DICT.get(obj['action_type'].upper())
    else:
        obj['action_type_description'] = None

    # Derive description for AssistanceType
    if obj['assistance_type']:
        obj['assistance_type_desc'] = ASSISTANCE_TYPE_DICT.get(obj['assistance_type'])
    else:
        obj['assistance_type_desc'] = None

    # Derive description for CorrectionDeleteIndicator
    if obj['correction_delete_indicatr']:
        obj['correction_delete_ind_desc'] = CORRECTION_DELETE_IND_DICT.get(obj['correction_delete_indicatr'].upper())
    else:
        obj['correction_delete_ind_desc'] = None

    # Derive description for RecordType
    if obj['record_type']:
        obj['record_type_description'] = RECORD_TYPE_DICT.get(obj['record_type'])
    else:
        obj['record_type_description'] = None

    # Derive description for BusinessTypes
    if obj['business_types']:
        types_list = []
        types_string = None
        # loop through all the entries in business_types
        for i in obj['business_types'].upper():
            type_desc = BUSINESS_TYPE_DICT.get(i)
            # If a valid business type was entered, append it to the list
            if type_desc:
                types_list.append(type_desc)
        # If there was at least one valid business type, turn it into a string, separated by semicolons
        if len(types_list) > 0:
            types_string = ";".join(types_list)
        obj['business_types_desc'] = types_string
    else:
        obj['business_types_desc'] = None

    # Derive description for BusinessFundsIndicator
    if obj['business_funds_indicator']:
        obj['business_funds_ind_desc'] = BUSINESS_FUNDS_IND_DICT.get(obj['business_funds_indicator'].upper())
    else:
        obj['business_funds_ind_desc'] = None


def set_active(obj):
    """ Setting active  """
    if obj['correction_delete_indicatr'] and obj['correction_delete_indicatr'].upper() == 'D':
        obj['is_active'] = False
    else:
        obj['is_active'] = True


def fabs_derivations(obj, sess, state_dict, country_dict, sub_tier_dict, cfda_dict, county_dict, fpds_office_dict):
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

    # deriving total_funding_amount
    federal_action_obligation = obj['federal_action_obligation'] or 0
    non_federal_funding_amount = obj['non_federal_funding_amount'] or 0
    obj['total_funding_amount'] = federal_action_obligation + non_federal_funding_amount

    derive_cfda(obj, cfda_dict, job_id, detached_award_financial_assistance_id)

    derive_awarding_agency_data(obj, sub_tier_dict)

    derive_funding_agency_data(obj, sub_tier_dict)

    ppop_code, ppop_state_code, ppop_state_name = derive_ppop_state(obj, state_dict)

    derive_ppop_location_data(obj, sess, ppop_code, ppop_state_code, county_dict)

    derive_le_location_data(obj, sess, ppop_code, state_dict, ppop_state_code, ppop_state_name, county_dict)

    # Deriving awarding_office_name based off awarding_office_code
    obj['awarding_office_name'] = fpds_office_dict.get(obj['awarding_office_code'])

    # Deriving funding_office_name based off funding_office_code
    obj['funding_office_name'] = fpds_office_dict.get(obj['funding_office_code'])

    derive_le_city_code(obj, sess)

    derive_ppop_country_name(obj, country_dict)

    derive_le_country_name(obj, country_dict)

    split_ppop_zip(obj)

    derive_parent_duns(obj, sess)

    derive_labels(obj)

    set_active(obj)

    # calculate business categories
    obj['business_categories'] = get_business_categories(row=obj, data_type='fabs')

    obj['modified_at'] = datetime.utcnow()

    return obj
