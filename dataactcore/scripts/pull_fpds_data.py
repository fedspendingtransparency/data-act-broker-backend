import boto3
import logging
import argparse
import requests
import xmltodict
import asyncio

import datetime
import time
import re
import json
import math

from sqlalchemy import func

from dateutil.relativedelta import relativedelta

from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import SubTierAgency, CountryCode, States, CountyCode, Zips, DUNS
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.jobModels import FPDSUpdate

from dataactcore.utils.business_categories import get_business_categories
from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter

feed_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&templateName=1.5.0&q="
delete_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=DELETED&templateName=1.5.0&q="
country_code_map = {'USA': 'US', 'ASM': 'AS', 'GUM': 'GU', 'MNP': 'MP', 'PRI': 'PR', 'VIR': 'VI', 'FSM': 'FM',
                    'MHL': 'MH', 'PLW': 'PW', 'XBK': 'UM', 'XHO': 'UM', 'XJV': 'UM', 'XJA': 'UM', 'XKR': 'UM',
                    'XPL': 'UM', 'XMW': 'UM', 'XWK': 'UM'}

FPDS_NAMESPACES = {'http://www.fpdsng.com/FPDS': None,
                   'http://www.w3.org/2005/Atom': None,
                   'https://www.fpds.gov/FPDS': None}

# Used for asyncio get requests against the ATOM feed
MAX_ENTRIES = 10
MAX_REQUESTS_AT_ONCE = 100
SPOT_CHECK_COUNT = 10000

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def list_data(data):
    if isinstance(data, dict):
        # make a list so it's consistent
        data = [data, ]
    return data


def extract_text(data_val):
    if type(data_val) is not str:
        data_val = data_val['#text']

    # If it's now a string, we want to strip it
    if type(data_val) is str:
        data_val = data_val.strip()
    return data_val


def is_valid_zip(zip_code):
    if re.match('^\d{5}(-?\d{4})?$', zip_code):
        return True
    return False


def get_county_by_zip(sess, zip_code):
    # if the zip code is not a valid US zip, toss the entire zip
    if not is_valid_zip(zip_code):
        return None

    zip_data = None
    # if we have a 9 digit code, grab the first match for 9 digit zips
    if len(zip_code) > 5:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5], zip_last4=zip_code[-4:]).first()

    # if it's not 9 digits or we found no results from the 9 digit we received
    if not zip_data:
        zip_data = sess.query(Zips).filter_by(zip5=zip_code[:5]).first()

    # if we found results at any point, return the county code from it
    if zip_data:
        return zip_data.county_number

    return None


def award_id_values(data, obj):
    """ Get values from the awardID level of the xml """
    value_map = {'modNumber': 'award_modification_amendme',
                 'transactionNumber': 'transaction_number',
                 'PIID': 'piid',
                 'agencyID': 'agency_id'}
    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['awardContractID'][key])
        except (KeyError, TypeError):
            obj[value] = None

    value_map = {'agencyID': 'referenced_idv_agency_iden',
                 'modNumber': 'referenced_idv_modificatio',
                 'PIID': 'parent_award_id'}
    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['referencedIDVID'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # get agencyID name
    try:
        obj['referenced_idv_agency_desc'] = extract_text(data['referencedIDVID']['agencyID']['@name'])
    except (KeyError, TypeError):
        obj['referenced_idv_agency_desc'] = None

    return obj


def contract_id_values(data, obj):
    """ Get values from the contractID level of the xml """
    value_map = {'modNumber': 'award_modification_amendme',
                 'PIID': 'piid',
                 'agencyID': 'agency_id'}
    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['IDVID'][key])
        except (KeyError, TypeError):
            obj[value] = None

    value_map = {'agencyID': 'referenced_idv_agency_iden',
                 'modNumber': 'referenced_idv_modificatio',
                 'PIID': 'parent_award_id'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['referencedIDVID'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # get agencyID name
    try:
        obj['referenced_idv_agency_desc'] = extract_text(data['referencedIDVID']['agencyID']['@name'])
    except (KeyError, TypeError):
        obj['referenced_idv_agency_desc'] = None

    return obj


def competition_values(data, obj):
    """ Get values from the competition level of the xml """
    value_map = {'A76Action': 'a_76_fair_act_action',
                 'commercialItemAcquisitionProcedures': 'commercial_item_acquisitio',
                 'commercialItemTestProgram': 'commercial_item_test_progr',
                 'evaluatedPreference': 'evaluated_preference',
                 'extentCompeted': 'extent_competed',
                 'fedBizOpps': 'fed_biz_opps',
                 'localAreaSetAside': 'local_area_set_aside',
                 'numberOfOffersReceived': 'number_of_offers_received',
                 'priceEvaluationPercentDifference': 'price_evaluation_adjustmen',
                 'reasonNotCompeted': 'other_than_full_and_open_c',
                 'research': 'research',
                 'smallBusinessCompetitivenessDemonstrationProgram': 'small_business_competitive',
                 'solicitationProcedures': 'solicitation_procedures',
                 'statutoryExceptionToFairOpportunity': 'fair_opportunity_limited_s',
                 'typeOfSetAside': 'type_set_aside'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'A76Action': 'a_76_fair_act_action_desc',
                 'commercialItemAcquisitionProcedures': 'commercial_item_acqui_desc',
                 'commercialItemTestProgram': 'commercial_item_test_desc',
                 'evaluatedPreference': 'evaluated_preference_desc',
                 'extentCompeted': 'extent_compete_description',
                 'fedBizOpps': 'fed_biz_opps_description',
                 'localAreaSetAside': 'local_area_set_aside_desc',
                 'reasonNotCompeted': 'other_than_full_and_o_desc',
                 'research': 'research_description',
                 'solicitationProcedures': 'solicitation_procedur_desc',
                 'statutoryExceptionToFairOpportunity': 'fair_opportunity_limi_desc',
                 'typeOfSetAside': 'type_set_aside_description'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def contract_data_values(data, obj, atom_type):
    """ Get values from the contractData level of the xml """
    value_map = {'consolidatedContract': 'consolidated_contract',
                 'contingencyHumanitarianPeacekeepingOperation': 'contingency_humanitarian_o',
                 'contractFinancing': 'contract_financing',
                 'costAccountingStandardsClause': 'cost_accounting_standards',
                 'costOrPricingData': 'cost_or_pricing_data',
                 'descriptionOfContractRequirement': 'award_description',
                 'GFE-GFP': 'government_furnished_prope',
                 'inherentlyGovernmentalFunction': 'inherently_government_func',
                 'majorProgramCode': 'major_program',
                 'multiYearContract': 'multi_year_contract',
                 'nationalInterestActionCode': 'national_interest_action',
                 'numberOfActions': 'number_of_actions',
                 'performanceBasedServiceContract': 'performance_based_service',
                 'programAcronym': 'program_acronym',
                 'purchaseCardAsPaymentMethod': 'purchase_card_as_payment_m',
                 'reasonForModification': 'action_type',
                 'referencedIDVMultipleOrSingle': 'referenced_mult_or_single',
                 'referencedIDVType': 'referenced_idv_type',
                 'seaTransportation': 'sea_transportation',
                 'solicitationID': 'solicitation_identifier',
                 'typeOfContractPricing': 'type_of_contract_pricing',
                 'typeOfIDC': 'type_of_idc',
                 'undefinitizedAction': 'undefinitized_action'}

    if atom_type == "award":
        value_map['contractActionType'] = 'contract_award_type'
    else:
        value_map['contractActionType'] = 'idv_type'
        value_map['multipleOrSingleAwardIDC'] = 'multiple_or_single_award_i'

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'consolidatedContract': 'consolidated_contract_desc',
                 'contingencyHumanitarianPeacekeepingOperation': 'contingency_humanitar_desc',
                 'contractFinancing': 'contract_financing_descrip',
                 'costAccountingStandardsClause': 'cost_accounting_stand_desc',
                 'costOrPricingData': 'cost_or_pricing_data_desc',
                 'GFE-GFP': 'government_furnished_desc',
                 'inherentlyGovernmentalFunction': 'inherently_government_desc',
                 'multiYearContract': 'multi_year_contract_desc',
                 'nationalInterestActionCode': 'national_interest_desc',
                 'performanceBasedServiceContract': 'performance_based_se_desc',
                 'purchaseCardAsPaymentMethod': 'purchase_card_as_paym_desc',
                 'reasonForModification': 'action_type_description',
                 'referencedIDVMultipleOrSingle': 'referenced_mult_or_si_desc',
                 'referencedIDVType': 'referenced_idv_type_desc',
                 'seaTransportation': 'sea_transportation_desc',
                 'typeOfContractPricing': 'type_of_contract_pric_desc',
                 'typeOfIDC': 'type_of_idc_description',
                 'undefinitizedAction': 'undefinitized_action_desc'}

    if atom_type == "award":
        value_map['contractActionType'] = 'contract_award_type_desc'
    else:
        value_map['contractActionType'] = 'idv_type_description'
        value_map['multipleOrSingleAwardIDC'] = 'multiple_or_single_aw_desc'

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def dollar_values_values(data, obj):
    """ Get values from the dollarValues level of the xml """
    value_map = {'baseAndAllOptionsValue': 'base_and_all_options_value',
                 'baseAndExercisedOptionsValue': 'base_exercised_options_val',
                 'obligatedAmount': 'federal_action_obligation'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def total_dollar_values_values(data, obj):
    """ Get values from the totalDollarValues level of the xml """
    value_map = {'totalBaseAndAllOptionsValue': 'potential_total_value_awar',
                 'totalBaseAndExercisedOptionsValue': 'current_total_value_award',
                 'totalObligatedAmount': 'total_obligated_amount'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def legislative_mandates_values(data, obj):
    """ Get values from the legislativeMandates level of the xml """
    value_map = {'ClingerCohenAct': 'clinger_cohen_act_planning',
                 'constructionWageRateRequirements': 'construction_wage_rate_req',
                 'interagencyContractingAuthority': 'interagency_contracting_au',
                 'otherStatutoryAuthority': 'other_statutory_authority',
                 'laborStandards': 'labor_standards',
                 'materialsSuppliesArticlesEquipment': 'materials_supplies_article'}

    additional_reporting = None
    try:
        ar_dicts = data['listOfAdditionalReportingValues']['additionalReportingValue']
    except (KeyError, TypeError):
        ar_dicts = None
    if ar_dicts:
        # if there is only one dict, convert it to a list of one dict
        if isinstance(ar_dicts, dict):
            ar_dicts = [ar_dicts]
        ars = []
        for ar_dict in ar_dicts:
            ar_value = extract_text(ar_dict)
            try:
                ar_desc = extract_text(ar_dict['@description'])
            except (KeyError, TypeError):
                ar_desc = None
            ar_str = ar_value if ar_desc is None else '{}: {}'.format(ar_value, ar_desc)
            ars.append(ar_str)
        additional_reporting = '; '.join(ars)
    obj['additional_reporting'] = additional_reporting

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'ClingerCohenAct': 'clinger_cohen_act_pla_desc',
                 'constructionWageRateRequirements': 'construction_wage_rat_desc',
                 'interagencyContractingAuthority': 'interagency_contract_desc',
                 'laborStandards': 'labor_standards_descrip',
                 'materialsSuppliesArticlesEquipment': 'materials_supplies_descrip'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def place_of_performance_values(data, obj):
    """ Get values from the placeOfPerformance level of the xml """
    value_map = {'placeOfPerformanceCongressionalDistrict': 'place_of_performance_congr',
                 'placeOfPerformanceZIPCode': 'place_of_performance_zip4a'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # placeOfPerformanceName
    try:
        obj['place_of_perform_city_name'] = extract_text(data['placeOfPerformanceZIPCode']['@city'])
    except (KeyError, TypeError):
        obj['place_of_perform_city_name'] = None

    # placeOfPerformanceName
    try:
        obj['place_of_perform_county_na'] = extract_text(data['placeOfPerformanceZIPCode']['@county'])
    except (KeyError, TypeError):
        obj['place_of_perform_county_na'] = None

    # within placeOfPerformance, the principalPlaceOfPerformance sub-level
    value_map = {'stateCode': 'place_of_performance_state',
                 'countryCode': 'place_of_perform_country_c'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['principalPlaceOfPerformance'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'countryCode': 'place_of_perf_country_desc',
                 'stateCode': 'place_of_perfor_state_desc'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['principalPlaceOfPerformance'][key]['@name'])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def product_or_service_information_values(data, obj):
    """ Get values from the productOrServiceInformation level of the xml """
    value_map = {'claimantProgramCode': 'dod_claimant_program_code',
                 'contractBundling': 'contract_bundling',
                 'countryOfOrigin': 'country_of_product_or_serv',
                 'informationTechnologyCommercialItemCategory': 'information_technology_com',
                 'manufacturingOrganizationType': 'domestic_or_foreign_entity',
                 'placeOfManufacture': 'place_of_manufacture',
                 'principalNAICSCode': 'naics',
                 'productOrServiceCode': 'product_or_service_code',
                 'recoveredMaterialClauses': 'recovered_materials_sustai',
                 'systemEquipmentCode': 'program_system_or_equipmen',
                 'useOfEPADesignatedProducts': 'epa_designated_product'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'claimantProgramCode': 'dod_claimant_prog_cod_desc',
                 'contractBundling': 'contract_bundling_descrip',
                 'informationTechnologyCommercialItemCategory': 'information_technolog_desc',
                 'manufacturingOrganizationType': 'domestic_or_foreign_e_desc',
                 'placeOfManufacture': 'place_of_manufacture_desc',
                 'principalNAICSCode': 'naics_description',
                 'productOrServiceCode': 'product_or_service_co_desc',
                 'recoveredMaterialClauses': 'recovered_materials_s_desc',
                 'systemEquipmentCode': 'program_system_or_equ_desc',
                 'useOfEPADesignatedProducts': 'epa_designated_produc_desc'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    # get country of origin name
    try:
        obj['country_of_product_or_desc'] = extract_text(data['countryOfOrigin']['@name'])
    except (KeyError, TypeError):
        obj['country_of_product_or_desc'] = None

    return obj


def purchaser_information_values(data, obj):
    """ Get values from the purchaserInformation level of the xml """
    value_map = {'contractingOfficeAgencyID': 'awarding_sub_tier_agency_c',
                 'contractingOfficeID': 'awarding_office_code',
                 'foreignFunding': 'foreign_funding',
                 'fundingRequestingAgencyID': 'funding_sub_tier_agency_co',
                 'fundingRequestingOfficeID': 'funding_office_code'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'foreignFunding': 'foreign_funding_desc'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    # name values associated with certain values in purchaserInformation
    value_map = {'contractingOfficeAgencyID': 'awarding_sub_tier_agency_n',
                 'contractingOfficeID': 'awarding_office_name',
                 'fundingRequestingAgencyID': 'funding_sub_tier_agency_na',
                 'fundingRequestingOfficeID': 'funding_office_name'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@name'])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def relevant_contract_dates_values(data, obj):
    """ Get values from the relevantContractDates level of the xml """
    value_map = {'currentCompletionDate': 'period_of_performance_curr',
                 'effectiveDate': 'period_of_performance_star',
                 'lastDateToOrder': 'ordering_period_end_date',
                 'signedDate': 'action_date',
                 'ultimateCompletionDate': 'period_of_perf_potential_e'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def vendor_values(data, obj):
    """ Get values from the vendor level of the xml """
    # base vendor level
    value_map = {'CCRException': 'sam_exception',
                 'contractingOfficerBusinessSizeDetermination': 'contracting_officers_deter'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'CCRException': 'sam_exception_description',
                 'contractingOfficerBusinessSizeDetermination': 'contracting_officers_desc'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key]['@description'])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorHeader sub-level
    value_map = {'vendorAlternateName': 'vendor_alternate_name',
                 'vendorDoingAsBusinessName': 'vendor_doing_as_business_n',
                 'vendorEnabled': 'vendor_enabled',
                 'vendorLegalOrganizationName': 'vendor_legal_org_name',
                 'vendorName': 'awardee_or_recipient_legal'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorHeader'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # make sure key exists before passing it
    try:
        data['vendorSiteDetails']
    except KeyError:
        data['vendorSiteDetails'] = {}
    # vendorSiteDetails sub-level (there are a lot so it gets its own function)
    obj = vendor_site_details_values(data['vendorSiteDetails'], obj)

    return obj


def vendor_site_details_values(data, obj):
    """ Get values from the vendorSiteDetails level of the xml (sub-level of vendor) """
    # base vendorSiteDetails level
    value_map = {'divisionName': 'division_name',
                 'divisionNumberOrOfficeCode': 'division_number_or_office',
                 'vendorAlternateSiteCode': 'vendor_alternate_site_code',
                 'vendorSiteCode': 'vendor_site_code'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # typeOfEducationalEntity sub-level
    value_map = {'is1862LandGrantCollege': 'c1862_land_grant_college',
                 'is1890LandGrantCollege': 'c1890_land_grant_college',
                 'is1994LandGrantCollege': 'c1994_land_grant_college',
                 'isAlaskanNativeServicingInstitution': 'alaskan_native_servicing_i',
                 'isHistoricallyBlackCollegeOrUniversity': 'historically_black_college',
                 'isMinorityInstitution': 'minority_institution',
                 'isNativeHawaiianServicingInstitution': 'native_hawaiian_servicing',
                 'isPrivateUniversityOrCollege': 'private_university_or_coll',
                 'isSchoolOfForestry': 'school_of_forestry',
                 'isStateControlledInstitutionofHigherLearning': 'state_controlled_instituti',
                 'isTribalCollege': 'tribal_college',
                 'isVeterinaryCollege': 'veterinary_college'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['typeOfEducationalEntity'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # typeOfGovernmentEntity sub-level
    value_map = {'isAirportAuthority': 'airport_authority',
                 'isCouncilOfGovernments': 'council_of_governments',
                 'isHousingAuthoritiesPublicOrTribal': 'housing_authorities_public',
                 'isInterstateEntity': 'interstate_entity',
                 'isPlanningCommission': 'planning_commission',
                 'isPortAuthority': 'port_authority',
                 'isTransitAuthority': 'transit_authority'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['typeOfGovernmentEntity'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorBusinessTypes sub-level
    value_map = {'isCommunityDevelopedCorporationOwnedFirm': 'community_developed_corpor',
                 'isForeignGovernment': 'foreign_government',
                 'isLaborSurplusAreaFirm': 'labor_surplus_area_firm',
                 'isStateGovernment': 'us_state_government',
                 'isTribalGovernment': 'us_tribal_government'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorBusinessTypes'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorBusinessTypes > businessOrOrganizationType sub-level
    value_map = {'isCorporateEntityNotTaxExempt': 'corporate_entity_not_tax_e',
                 'isCorporateEntityTaxExempt': 'corporate_entity_tax_exemp',
                 'isInternationalOrganization': 'international_organization',
                 'isPartnershipOrLimitedLiabilityPartnership': 'partnership_or_limited_lia',
                 'isSmallAgriculturalCooperative': 'small_agricultural_coopera',
                 'isSolePropreitorship': 'sole_proprietorship',
                 'isUSGovernmentEntity': 'us_government_entity'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorBusinessTypes']['businessOrOrganizationType'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorBusinessTypes > federalGovernment sub-level
    value_map = {'isFederalGovernment': 'us_federal_government',
                 'isFederalGovernmentAgency': 'federal_agency',
                 'isFederallyFundedResearchAndDevelopmentCorp': 'federally_funded_research'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorBusinessTypes']['federalGovernment'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorBusinessTypes > localGovernment sub-level
    value_map = {'isCityLocalGovernment': 'city_local_government',
                 'isCountyLocalGovernment': 'county_local_government',
                 'isInterMunicipalLocalGovernment': 'inter_municipal_local_gove',
                 'isLocalGovernment': 'us_local_government',
                 'isLocalGovernmentOwned': 'local_government_owned',
                 'isMunicipalityLocalGovernment': 'municipality_local_governm',
                 'isSchoolDistrictLocalGovernment': 'school_district_local_gove',
                 'isTownshipLocalGovernment': 'township_local_government'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorBusinessTypes']['localGovernment'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorCertifications sub-level
    value_map = {'isDOTCertifiedDisadvantagedBusinessEnterprise': 'dot_certified_disadvantage',
                 'isSBACertified8AJointVenture': 'sba_certified_8_a_joint_ve',
                 'isSBACertified8AProgramParticipant': 'c8a_program_participant',
                 'isSBACertifiedHUBZone': 'historically_underutilized',
                 'isSBACertifiedSmallDisadvantagedBusiness': 'small_disadvantaged_busine',
                 'isSelfCertifiedSmallDisadvantagedBusiness': 'self_certified_small_disad'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorCertifications'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorDUNSInformation sub-level
    value_map = {'cageCode': 'cage_code',
                 'DUNSNumber': 'awardee_or_recipient_uniqu',
                 'globalParentDUNSName': 'ultimate_parent_legal_enti',
                 'globalParentDUNSNumber': 'ultimate_parent_unique_ide'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorDUNSInformation'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorLineOfBusiness sub-level
    value_map = {'isCommunityDevelopmentCorporation': 'community_development_corp',
                 'isDomesticShelter': 'domestic_shelter',
                 'isEducationalInstitution': 'educational_institution',
                 'isFoundation': 'foundation',
                 'isHispanicServicingInstitution': 'hispanic_servicing_institu',
                 'isHospital': 'hospital_flag',
                 'isManufacturerOfGoods': 'manufacturer_of_goods',
                 'isVeterinaryHospital': 'veterinary_hospital'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorLineOfBusiness'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorLocation sub-level
    value_map = {'city': 'legal_entity_city_name',
                 'congressionalDistrictCode': 'legal_entity_congressional',
                 'countryCode': 'legal_entity_country_code',
                 'faxNo': 'vendor_fax_number',
                 'phoneNo': 'vendor_phone_number',
                 'streetAddress': 'legal_entity_address_line1',
                 'streetAddress2': 'legal_entity_address_line2',
                 'streetAddress3': 'legal_entity_address_line3',
                 'vendorLocationDisabledFlag': 'vendor_location_disabled_f',
                 'ZIPCode': 'legal_entity_zip4'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorLocation'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # differentiating between US and foreign states
    key = 'legal_entity_state_code'
    if obj['legal_entity_country_code'] not in country_code_map:
        key = 'legal_entity_state_descrip'
        # need to set this even if we're not going to be having a code because we need to access it later
        obj['legal_entity_state_code'] = None
    # if it is in the USA, grab the description for the state
    else:
        try:
            obj['legal_entity_state_descrip'] = extract_text(data['vendorLocation']['state']['@name'])
        except (KeyError, TypeError):
            obj['legal_entity_state_descrip'] = None

    try:
        obj[key] = extract_text(data['vendorLocation']['state'])
    except (KeyError, TypeError):
        obj[key] = None

    # getting the name associated with the country code
    try:
        obj['legal_entity_country_name'] = extract_text(data['vendorLocation']['countryCode']['@name'])
    except (KeyError, TypeError):
        obj['legal_entity_country_name'] = None

    # vendorOrganizationFactors sub-level
    value_map = {'isForeignOwnedAndLocated': 'foreign_owned_and_located',
                 'isLimitedLiabilityCorporation': 'limited_liability_corporat',
                 'isShelteredWorkshop': 'the_ability_one_program',
                 'isSubchapterSCorporation': 'subchapter_s_corporation',
                 'organizationalType': 'organizational_type'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorOrganizationFactors'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorOrganizationFactors > profitStructure sub-level
    value_map = {'isForProfitOrganization': 'for_profit_organization',
                 'isNonprofitOrganization': 'nonprofit_organization',
                 'isOtherNotForProfitOrganization': 'other_not_for_profit_organ'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorOrganizationFactors']['profitStructure'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorRelationshipWithFederalGovernment sub-level
    value_map = {'receivesContracts': 'contracts',
                 'receivesContractsAndGrants': 'receives_contracts_and_gra',
                 'receivesGrants': 'grants'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorRelationshipWithFederalGovernment'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorSocioEconomicIndicators sub-level
    value_map = {'isAlaskanNativeOwnedCorporationOrFirm': 'alaskan_native_owned_corpo',
                 'isAmericanIndianOwned': 'american_indian_owned_busi',
                 'isEconomicallyDisadvantagedWomenOwnedSmallBusiness': 'economically_disadvantaged',
                 'isIndianTribe': 'indian_tribe_federally_rec',
                 'isJointVentureEconomicallyDisadvantagedWomenOwnedSmallBusiness': 'joint_venture_economically',
                 'isJointVentureWomenOwnedSmallBusiness': 'joint_venture_women_owned',
                 'isNativeHawaiianOwnedOrganizationOrFirm': 'native_hawaiian_owned_busi',
                 'isServiceRelatedDisabledVeteranOwnedBusiness': 'service_disabled_veteran_o',
                 'isTriballyOwnedFirm': 'tribally_owned_business',
                 'isVerySmallBusiness': 'emerging_small_business',
                 'isVeteranOwned': 'veteran_owned_business',
                 'isWomenOwned': 'woman_owned_business',
                 'isWomenOwnedSmallBusiness': 'women_owned_small_business'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorSocioEconomicIndicators'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # vendorSocioEconomicIndicators > minorityOwned sub-level
    value_map = {'isAsianPacificAmericanOwnedBusiness': 'asian_pacific_american_own',
                 'isBlackAmericanOwnedBusiness': 'black_american_owned_busin',
                 'isHispanicAmericanOwnedBusiness': 'hispanic_american_owned_bu',
                 'isMinorityOwned': 'minority_owned_business',
                 'isNativeAmericanOwnedBusiness': 'native_american_owned_busi',
                 'isOtherMinorityOwned': 'other_minority_owned_busin',
                 'isSubContinentAsianAmericanOwnedBusiness': 'subcontinent_asian_asian_i'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorSocioEconomicIndicators']['minorityOwned'][key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def generic_values(data, obj):
    """ Get values from the genericTags level of the xml """
    generic_strings_value_map = {'genericString01': 'solicitation_date'}

    for key, value in generic_strings_value_map.items():
        try:
            obj[value] = extract_text(data['genericStrings'][key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


def calculate_ppop_fields(obj, sess, county_by_name, county_by_code, state_code_list, country_list):
    """ calculate values that aren't in any feed (or haven't been provided properly) for place of performance """
    # only do any of these calculation if the country code is in the list of US territories
    if obj['place_of_perform_country_c'] in country_code_map:
        # If it's in the list but not USA, find its state code in the list and put that in the state code spot, get
        # the state name, then replace country code and country description with USA and UNITED STATES respectively
        if obj['place_of_perform_country_c'] != 'USA':
            obj['place_of_performance_state'] = country_code_map[obj['place_of_perform_country_c']]
            if obj['place_of_performance_state'] in state_code_list:
                obj['place_of_perfor_state_desc'] = state_code_list[obj['place_of_performance_state']]
            obj['place_of_perform_country_c'] = 'USA'
            obj['place_of_perf_country_desc'] = 'UNITED STATES'

        # derive state name if we don't have it
        if obj['place_of_performance_state'] and not obj['place_of_perfor_state_desc']\
                and obj['place_of_performance_state'] in state_code_list:
            obj['place_of_perfor_state_desc'] = state_code_list[obj['place_of_performance_state']]

        # calculate place of performance county code
        if obj['place_of_perform_county_na'] and obj['place_of_performance_state']:
            state = obj['place_of_performance_state']
            county_name = obj['place_of_perform_county_na']
            # make sure they gave us a valid state and then check if it's in our lookup
            if state in county_by_name and county_name in county_by_name[state]:
                obj['place_of_perform_county_co'] = county_by_name[state][county_name]

        # if accessing the county code by state code and county name didn't work, try by zip4a if we have it
        if not obj['place_of_perform_county_co'] and obj['place_of_performance_zip4a']:
            obj['place_of_perform_county_co'] = get_county_by_zip(sess, obj['place_of_performance_zip4a'])

        # if we didn't have a county name but got the county code, we can grab the name
        if not obj['place_of_perform_county_na'] and obj['place_of_performance_state'] in county_by_code\
                and obj['place_of_perform_county_co'] in county_by_code[obj['place_of_performance_state']]:
            obj['place_of_perform_county_na'] =\
                county_by_code[obj['place_of_performance_state']][obj['place_of_perform_county_co']]

        # if we have content in the zip code and it's in a valid US format, split it into 5 and 4 digit
        if obj['place_of_performance_zip4a'] and is_valid_zip(obj['place_of_performance_zip4a']):
            obj['place_of_performance_zip5'] = obj['place_of_performance_zip4a'][:5]
            if len(obj['place_of_performance_zip4a']) > 5:
                obj['place_of_perform_zip_last4'] = obj['place_of_performance_zip4a'][-4:]

    # if there is any country code (checked outside function) but not a country name, try to get the country name
    if not obj['place_of_perf_country_desc'] and obj['place_of_perform_country_c'] in country_list:
        obj['place_of_perf_country_desc'] = country_list[obj['place_of_perform_country_c']]


def calculate_legal_entity_fields(obj, sess, county_by_code, state_code_list, country_list):
    """ calculate values that aren't in any feed (or haven't been provided properly) for legal entity """
    # do legal entity derivations only if legal entity country code is in a US territory of any kind
    if obj['legal_entity_country_code'] in country_code_map:
        # if it's in the list but not USA, find its state code in the list and put that in the state code spot, get
        # the state name, then replace country code and country description with USA and UNITED STATES respectively
        if obj['legal_entity_country_code'] != 'USA':
            obj['legal_entity_state_code'] = country_code_map[obj['legal_entity_country_code']]
            if obj['legal_entity_state_code'] in state_code_list:
                obj['legal_entity_state_descrip'] = state_code_list[obj['legal_entity_state_code']]
            obj['legal_entity_country_code'] = 'USA'
            obj['legal_entity_country_name'] = 'UNITED STATES'

        # derive state name if we don't have it
        if obj['legal_entity_state_code'] and not obj['legal_entity_state_descrip']\
                and obj['legal_entity_state_code'] in state_code_list:
            obj['legal_entity_state_descrip'] = state_code_list[obj['legal_entity_state_code']]

        # calculate legal entity county code and split zip when possible
        if obj['legal_entity_zip4'] and is_valid_zip(obj['legal_entity_zip4']):
            obj['legal_entity_county_code'] = get_county_by_zip(sess, obj['legal_entity_zip4'])

            # if we have a county code and a state code, we can try to get the county name
            if obj['legal_entity_county_code'] and obj['legal_entity_state_code']:
                county_code = obj['legal_entity_county_code']
                state = obj['legal_entity_state_code']

                # make sure they gave us a valid state and then check if it's in our lookup
                if state in county_by_code and county_code in county_by_code[state]:
                    obj['legal_entity_county_name'] = county_by_code[state][county_code]

            obj['legal_entity_zip5'] = obj['legal_entity_zip4'][:5]
            if len(obj['legal_entity_zip4']) > 5:
                obj['legal_entity_zip_last4'] = obj['legal_entity_zip4'][-4:]

    # if there is any country code (checked outside function) but not a country name, try to get the country name
    if not obj['legal_entity_country_name'] and obj['legal_entity_country_code'] in country_list:
        obj['legal_entity_country_name'] = country_list[obj['legal_entity_country_code']]


def calculate_remaining_fields(obj, sess, sub_tier_list, county_by_name, county_by_code, state_code_list, country_list,
                               exec_comp_dict, atom_type):
    """ Calculate values that aren't in any feed but can be calculated.

        Args:
            obj: a dictionary containing the details we need to derive from and to
            sess: the database connection
            sub_tier_list: a dictionary containing all the sub tier agency information keyed by sub tier agency code
            county_by_name: a dictionary containing all county codes, keyed by state and county name
            county_by_code: a dictionary containing all county names, keyed by state and county code
            state_code_list: a dictionary containing all state names, keyed by state code
            country_list: a dictionary containing all country names, keyed by country code
            exec_comp_dict: a dictionary containing all the data for Executive Compensation data keyed by DUNS number
            atom_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'

        Returns:
            the object originally passed in with newly-calculated values added
    """
    # we want to null out all the calculated columns in case this is an update to the records
    obj['awarding_agency_code'] = None
    obj['awarding_agency_name'] = None
    obj['funding_agency_code'] = None
    obj['funding_agency_name'] = None
    obj['place_of_perform_county_co'] = None
    obj['legal_entity_county_code'] = None
    obj['legal_entity_county_name'] = None
    obj['detached_award_proc_unique'] = None

    # calculate awarding agency codes/names based on awarding sub tier agency codes
    if obj['awarding_sub_tier_agency_c']:
        try:
            sub_tier_agency = sub_tier_list[obj['awarding_sub_tier_agency_c']]
            use_frec = sub_tier_agency.is_frec
            agency_data = sub_tier_agency.frec if use_frec else sub_tier_agency.cgac
            obj['awarding_agency_code'] = agency_data.frec_code if use_frec else agency_data.cgac_code
            obj['awarding_agency_name'] = agency_data.agency_name
        except KeyError:
            logger.info('WARNING: MissingSubtierCGAC: The awarding sub-tier cgac_code: %s does not exist in cgac table.'
                        ' The FPDS-provided awarding sub-tier agency name (if given) for this cgac_code is %s. '
                        'The award has been loaded with awarding_agency_code 999.',
                        obj['awarding_sub_tier_agency_c'], obj['awarding_sub_tier_agency_n'])
            obj['awarding_agency_code'] = '999'
            obj['awarding_agency_name'] = None

    # calculate funding agency codes/names based on funding sub tier agency codes
    if obj['funding_sub_tier_agency_co']:
        try:
            sub_tier_agency = sub_tier_list[obj['funding_sub_tier_agency_co']]
            use_frec = sub_tier_agency.is_frec
            agency_data = sub_tier_agency.frec if use_frec else sub_tier_agency.cgac
            obj['funding_agency_code'] = agency_data.frec_code if use_frec else agency_data.cgac_code
            obj['funding_agency_name'] = agency_data.agency_name
        except KeyError:
            logger.info('WARNING: MissingSubtierCGAC: The funding sub-tier cgac_code: %s does not exist in cgac table. '
                        'The FPDS-provided funding sub-tier agency name (if given) for this cgac_code is %s. '
                        'The award has been loaded with funding_agency_code 999.',
                        obj['funding_sub_tier_agency_co'], obj['funding_sub_tier_agency_na'])
            obj['funding_agency_code'] = '999'
            obj['funding_agency_name'] = None

    # do place of performance calculations only if we have SOME country code
    if obj['place_of_perform_country_c']:
        calculate_ppop_fields(obj, sess, county_by_name, county_by_code, state_code_list, country_list)

    # do legal entity calculations only if we have SOME country code
    if obj['legal_entity_country_code']:
        calculate_legal_entity_fields(obj, sess, county_by_code, state_code_list, country_list)

    # calculate business categories
    obj['business_categories'] = get_business_categories(row=obj, data_type='fpds')

    # Calculate executive compensation data for the entry.
    if obj['awardee_or_recipient_uniqu'] and obj['awardee_or_recipient_uniqu'] in exec_comp_dict.keys():
        exec_comp = exec_comp_dict[obj['awardee_or_recipient_uniqu']]
        for i in range(1, 6):
            obj['high_comp_officer{}_full_na'.format(i)] = exec_comp['officer{}_name'.format(i)]
            obj['high_comp_officer{}_amount'.format(i)] = exec_comp['officer{}_amt'.format(i)]
    else:
        # Need to make sure they're null in case this is updating and the DUNS has changed somehow
        for i in range(1, 6):
            obj['high_comp_officer{}_full_na'.format(i)] = None
            obj['high_comp_officer{}_amount'.format(i)] = None

    # calculate unique award key
    if atom_type == 'award':
        unique_award_string_list = ['CONT_AWD']
        key_list = ['piid', 'agency_id', 'parent_award_id', 'referenced_idv_agency_iden']
    else:
        unique_award_string_list = ['CONT_IDV']
        key_list = ['piid', 'agency_id']
    for item in key_list:
        # Get the value in the object or, if the key doesn't exist or value is None, set it to "-none-"
        unique_award_string_list.append(obj.get(item) or '-none-')
    obj['unique_award_key'] = '_'.join(unique_award_string_list).upper()

    # calculate unique key
    key_list = ['agency_id', 'referenced_idv_agency_iden', 'piid', 'award_modification_amendme', 'parent_award_id',
                'transaction_number']
    idv_list = ['agency_id', 'piid', 'award_modification_amendme']
    unique_string = ""
    for item in key_list:
        if len(unique_string) > 0:
            unique_string += "_"

        if atom_type == 'award' or item in idv_list:
            # Get the value in the object or, if the key doesn't exist or value is None, set it to "-none-"
            unique_string += obj.get(item) or '-none-'
        else:
            unique_string += '-none-'

    # The order of the unique key is agency_id, referenced_idv_agency_iden, piid, award_modification_amendme,
    # parent_award_id, transaction_number
    obj['detached_award_proc_unique'] = unique_string
    return obj


def process_data(data, sess, atom_type, sub_tier_list, county_by_name, county_by_code, state_code_list, country_list,
                 exec_comp_dict):
    """ Process the data coming in.

        Args:
            data: an object containing the data gathered from the feed
            sess: the database connection
            atom_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
            sub_tier_list: a dictionary containing all the sub tier agency information keyed by sub tier agency code
            county_by_name: a dictionary containing all county codes, keyed by state and county name
            county_by_code: a dictionary containing all county names, keyed by state and county code
            state_code_list: a dictionary containing all state names, keyed by state code
            country_list: a dictionary containing all country names, keyed by country code
            exec_comp_dict: a dictionary containing all the data for Executive Compensation data keyed by DUNS number

        Returns:
            An object containing the processed and calculated data.
    """
    obj = {}

    if atom_type == "award":
        # make sure key exists before passing it
        try:
            data['awardID']
        except KeyError:
            data['awardID'] = {}
        obj = award_id_values(data['awardID'], obj)
    else:
        # transaction_number is a part of the unique identifier, set it to None
        obj['transaction_number'] = None
        # make sure key exists before passing it
        try:
            data['contractID']
        except KeyError:
            data['contractID'] = {}
        obj = contract_id_values(data['contractID'], obj)

    # make sure key exists before passing it
    try:
        data['competition']
    except KeyError:
        data['competition'] = {}
    obj = competition_values(data['competition'], obj)

    # make sure key exists before passing it
    try:
        data['contractData']
    except KeyError:
        data['contractData'] = {}
    obj = contract_data_values(data['contractData'], obj, atom_type)

    # make sure key exists before passing it
    try:
        data['dollarValues']
    except KeyError:
        data['dollarValues'] = {}
    obj = dollar_values_values(data['dollarValues'], obj)

    # make sure key exists before passing it
    try:
        data['totalDollarValues']
    except KeyError:
        data['totalDollarValues'] = {}
    obj = total_dollar_values_values(data['totalDollarValues'], obj)

    if atom_type == "award":
        # make sure key exists before passing it
        try:
            data['placeOfPerformance']
        except KeyError:
            data['placeOfPerformance'] = {}
        obj = place_of_performance_values(data['placeOfPerformance'], obj)
    # these values need to be filled so the existence check when calculating county data doesn't freak out
    else:
        obj['place_of_perform_county_na'] = None
        obj['place_of_performance_state'] = None
        obj['place_of_perfor_state_desc'] = None
        obj['place_of_performance_zip4a'] = None
        obj['place_of_perform_country_c'] = None
        obj['place_of_perf_country_desc'] = None

    # make sure key exists before passing it
    try:
        data['legislativeMandates']
    except KeyError:
        data['legislativeMandates'] = {}
    obj = legislative_mandates_values(data['legislativeMandates'], obj)

    try:
        obj['subcontracting_plan'] = extract_text(data['preferencePrograms']['subcontractPlan'])
    except (KeyError, TypeError):
        obj['subcontracting_plan'] = None

    try:
        obj['subcontracting_plan_desc'] = extract_text(data['preferencePrograms']['subcontractPlan']['@description'])
    except (KeyError, TypeError):
        obj['subcontracting_plan_desc'] = None

    # make sure key exists before passing it
    try:
        data['productOrServiceInformation']
    except KeyError:
        data['productOrServiceInformation'] = {}
    obj = product_or_service_information_values(data['productOrServiceInformation'], obj)

    # make sure key exists before passing it
    try:
        data['purchaserInformation']
    except KeyError:
        data['purchaserInformation'] = {}
    obj = purchaser_information_values(data['purchaserInformation'], obj)

    # make sure key exists before passing it
    try:
        data['relevantContractDates']
    except KeyError:
        data['relevantContractDates'] = {}
    obj = relevant_contract_dates_values(data['relevantContractDates'], obj)

    # make sure key exists before passing it
    try:
        data['vendor']
    except KeyError:
        data['vendor'] = {}
    obj = vendor_values(data['vendor'], obj)

    # make sure key exists before passing it
    try:
        data['genericTags']
    except KeyError:
        data['genericTags'] = {}
    obj = generic_values(data['genericTags'], obj)

    obj = calculate_remaining_fields(obj, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                                     country_list, exec_comp_dict, atom_type)

    try:
        obj['last_modified'] = extract_text(data['transactionInformation']['lastModifiedDate'])
    except (KeyError, TypeError):
        obj['last_modified'] = None

    try:
        obj['initial_report_date'] = extract_text(data['transactionInformation']['createdDate'])
    except (KeyError, TypeError):
        obj['initial_report_date'] = None

    obj['pulled_from'] = atom_type

    # clear out potentially excel-breaking whitespace from specific fields
    free_fields = ["award_description", "vendor_doing_as_business_n", "legal_entity_address_line1",
                   "legal_entity_address_line2", "legal_entity_address_line3", "ultimate_parent_legal_enti",
                   "awardee_or_recipient_legal", "other_statutory_authority"]
    for field in free_fields:
        if obj[field]:
            obj[field] = re.sub('\s', ' ', obj[field])

    return obj


def process_delete_data(data, atom_type):
    """ process the delete feed data coming in """
    unique_string = ""

    # order of unique constraints in string: agency_id, referenced_idv_agency_iden, piid, award_modification_amendme,
    # parent_award_id, transaction_number

    # get all values that make up unique key
    if atom_type == "award":
        try:
            unique_string += extract_text(data['awardID']['awardContractID']['agencyID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['awardID']['referencedIDVID']['agencyID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['awardID']['awardContractID']['PIID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['awardID']['awardContractID']['modNumber'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['awardID']['referencedIDVID']['PIID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['awardID']['awardContractID']['transactionNumber'])
        except (KeyError, TypeError):
            unique_string += "-none-"
    else:
        try:
            unique_string += extract_text(data['contractID']['IDVID']['agencyID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        # referenced_idv_agency_iden not used in IDV identifier, just set it to "-none-"
        unique_string += "_-none-_"

        try:
            unique_string += extract_text(data['contractID']['IDVID']['PIID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['contractID']['IDVID']['modNumber'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        # parent_award_id not used in IDV identifier and transaction_number not in IDV feed, just set them to "-none-"
        unique_string += "_-none-_-none-"

    return unique_string


def create_processed_data_list(data, contract_type, sess, sub_tier_list, county_by_name, county_by_code,
                               state_code_list, country_list, exec_comp_dict):
    """ Create a list of processed data

        Args:
            data: an object containing the data gathered from the feed
            sess: the database connection
            contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
            sub_tier_list: a dictionary containing all the sub tier agency information keyed by sub tier agency code
            county_by_name: a dictionary containing all county codes, keyed by state and county name
            county_by_code: a dictionary containing all county names, keyed by state and county code
            state_code_list: a dictionary containing all state names, keyed by state code
            country_list: a dictionary containing all country names, keyed by country code
            exec_comp_dict: a dictionary containing all the data for Executive Compensation data keyed by DUNS number

        Returns:
            A list containing the processed and calculated data.
    """
    data_list = []
    for value in data:
        tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                               sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                               county_by_code=county_by_code, state_code_list=state_code_list,
                               country_list=country_list, exec_comp_dict=exec_comp_dict)
        data_list.append(tmp_obj)
    return data_list


def add_processed_data_list(data, sess):
    try:
        sess.bulk_save_objects([DetachedAwardProcurement(**fpds_data) for fpds_data in data])
        sess.commit()
    except IntegrityError:
        sess.rollback()
        logger.error("Attempted to insert duplicate FPDS data. Inserting each row in batch individually.")

        for fpds_obj in data:
            insert_statement = insert(DetachedAwardProcurement).values(**fpds_obj).\
                on_conflict_do_update(index_elements=['detached_award_proc_unique'], set_=fpds_obj)
            sess.execute(insert_statement)
        sess.commit()


def process_and_add(data, contract_type, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                    country_list, exec_comp_dict, now, threaded=False):
    """ Start the processing for data and add it to the DB.

        Args:
            data: an object containing the data gathered from the feed
            contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
            sess: the database connection
            sub_tier_list: a dictionary containing all the sub tier agency information keyed by sub tier agency code
            county_by_name: a dictionary containing all county codes, keyed by state and county name
            county_by_code: a dictionary containing all county names, keyed by state and county code
            state_code_list: a dictionary containing all state names, keyed by state code
            country_list: a dictionary containing all country names, keyed by country code
            exec_comp_dict: a dictionary containing all the data for Executive Compensation data keyed by DUNS number
            now: a timestamp indicating the time to set the updated_at to
            threaded: a boolean indicating whether the process is running as a thread or not
    """
    if threaded:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                                   sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                                   county_by_code=county_by_code, state_code_list=state_code_list,
                                   country_list=country_list, exec_comp_dict=exec_comp_dict)
            tmp_obj['updated_at'] = now

            insert_statement = insert(DetachedAwardProcurement).values(**tmp_obj).\
                on_conflict_do_update(index_elements=['detached_award_proc_unique'], set_=tmp_obj)
            sess.execute(insert_statement)
    else:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                                   sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                                   county_by_code=county_by_code, state_code_list=state_code_list,
                                   country_list=country_list, exec_comp_dict=exec_comp_dict)

            try:
                statement = insert(DetachedAwardProcurement).values(**tmp_obj)
                sess.execute(statement)
                sess.commit()
            except IntegrityError:
                sess.rollback()
                tmp_obj['updated_at'] = now
                sess.query(DetachedAwardProcurement).\
                    filter_by(detached_award_proc_unique=tmp_obj['detached_award_proc_unique']).\
                    update(tmp_obj, synchronize_session=False)
                sess.commit()


def get_with_exception_hand(url_string):
    """ Retrieve data from FPDS, allow for multiple retries and timeouts """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while exception_retries < len(retry_sleep_times):
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            # we should always expect entries, otherwise we shouldn't be calling it
            resp_dict = xmltodict.parse(resp.text, process_namespaces=True, namespaces=FPDS_NAMESPACES)
            len(list_data(resp_dict['feed']['entry']))
            break
        except (ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout, KeyError) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception. Sleeping {}s and then retrying with a max wait of {}s...'
                            .format(retry_sleep_times[exception_retries], request_timeout))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                raise e
    return resp


def get_total_expected_records(base_url):
    """ Retrieve the total number of expected records based on the last paginated URL """
    # get a single call so we can find the last page
    initial_request = get_with_exception_hand(base_url)
    initial_request_xml = xmltodict.parse(initial_request.text, process_namespaces=True, namespaces=FPDS_NAMESPACES)

    # retrieve all URLs
    try:
        urls_list = list_data(initial_request_xml['feed']['link'])
    except KeyError:
        urls_list = []

    # retrieve the "last" URL from the list
    final_request_url = None
    for url in urls_list:
        if url['@rel'] == 'last':
            final_request_url = url['@href']
            continue

    # retrieve the count from the URL of the last page
    if not final_request_url:
        try:
            return len(list_data(initial_request_xml['feed']['entry']))
        except KeyError:
            return 0

    # retrieve the page from the final_request_url
    final_request_count = int(final_request_url.split('&start=')[-1])

    # retrieve the last page of data
    final_request = get_with_exception_hand(final_request_url)
    final_request_xml = xmltodict.parse(final_request.text, process_namespaces=True, namespaces=FPDS_NAMESPACES)
    try:
        entries_list = list_data(final_request_xml['feed']['entry'])
    except KeyError:
        raise Exception("Initial count failed, no entries in last page of request.")

    return final_request_count + len(entries_list)


def get_data(contract_type, award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
             country_list, exec_comp_dict, last_run=None, threaded=False, start_date=None, end_date=None, metrics=None,
             specific_params=None):
    """ Get the data from the atom feed based on contract/award type and the last time the script was run.

        Args:
            contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
            award_type: a string indicating what the award type of the feed being checked is
            now: a timestamp indicating the time to set the updated_at to
            sess: the database connection
            sub_tier_list: a dictionary containing all the sub tier agency information keyed by sub tier agency code
            county_by_name: a dictionary containing all county codes, keyed by state and county name
            county_by_code: a dictionary containing all county names, keyed by state and county code
            state_code_list: a dictionary containing all state names, keyed by state code
            country_list: a dictionary containing all country names, keyed by country code
            exec_comp_dict: a dictionary containing all the data for Executive Compensation data keyed by DUNS number
            last_run: a date indicating the last time the pull was run
            threaded: a boolean indicating whether the process is running as a thread or not
            start_date: a date indicating the first date to pull from (must be provided with end_date)
            end_date: a date indicating the last date to pull from (must be provided with start_date)
            metrics: a dictionary to gather metrics for the script in
            specific_params: a string containing a specific set of params to run the query with (used for outside
                scripts that need to run a data load)
    """
    if not metrics:
        metrics = {}
    data = []
    yesterday = now - datetime.timedelta(days=1)
    utcnow = datetime.datetime.utcnow()
    # If a specific set of params was provided, use that
    if specific_params:
        params = specific_params
    # if a date that the script was last successfully run is not provided, get all data
    elif not last_run:
        params = 'SIGNED_DATE:[2016/10/01,' + yesterday.strftime('%Y/%m/%d') + '] '
        metrics['start_date'] = '2016/10/01'
        metrics['end_date'] = yesterday.strftime('%Y/%m/%d')
    # if a date that the script was last successfully run is provided, get data since that date
    else:
        last_run_date = last_run - relativedelta(days=1)
        params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '
        metrics['start_date'] = last_run_date.strftime('%Y/%m/%d')
        metrics['end_date'] = yesterday.strftime('%Y/%m/%d')
        if start_date and end_date:
            params = 'LAST_MOD_DATE:[' + start_date + ',' + end_date + '] '
            metrics['start_date'] = start_date
            metrics['end_date'] = end_date

    base_url = feed_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '" AWARD_TYPE:"' + award_type + '"'
    logger.info('Starting get feed: %s', base_url)

    # retrieve the total count of expected records for this pull
    total_expected_records = get_total_expected_records(base_url)
    logger.info('{} record(s) expected from this feed'.format(total_expected_records))

    entries_processed = 0
    while True:
        # pull in the next MAX_ENTRIES * REQUESTS_AT_ONCE until we get anything less than the MAX_ENTRIES
        async def atom_async_get(entries_already_processed, total_expected_records):
            response_list = []
            loop = asyncio.get_event_loop()
            requests_at_once = MAX_REQUESTS_AT_ONCE
            if total_expected_records - entries_already_processed < (MAX_REQUESTS_AT_ONCE * MAX_ENTRIES):
                requests_at_once = math.ceil((total_expected_records-entries_already_processed)/MAX_ENTRIES)

            futures = [
                loop.run_in_executor(
                    None,
                    get_with_exception_hand,
                    base_url + "&start=" + str(entries_already_processed + (start_offset * MAX_ENTRIES))
                )
                for start_offset in range(requests_at_once)
            ]
            for response in await asyncio.gather(*futures):
                response_list.append(response.text)
                pass
            return response_list
        # End async get requests def

        loop = asyncio.get_event_loop()
        full_response = loop.run_until_complete(atom_async_get(entries_processed, total_expected_records))

        for next_resp in full_response:
            response_dict = xmltodict.parse(next_resp, process_namespaces=True, namespaces=FPDS_NAMESPACES)
            try:
                entries_per_response = list_data(response_dict['feed']['entry'])
            except KeyError:
                continue

            if last_run or specific_params:
                for entry in entries_per_response:
                    data.append(entry)
                    entries_processed += 1
            else:
                data.extend(create_processed_data_list(entries_per_response, contract_type, sess, sub_tier_list,
                                                       county_by_name, county_by_code, state_code_list, country_list,
                                                       exec_comp_dict))
                entries_processed += len(entries_per_response)

        if len(data) % SPOT_CHECK_COUNT == 0 and entries_processed > total_expected_records:
            # Find entries that don't have FPDS content and print them all
            for next_resp in full_response:
                response_dict = xmltodict.parse(next_resp, process_namespaces=True, namespaces=FPDS_NAMESPACES)
                try:
                    list_data(response_dict['feed']['entry'])
                except KeyError:
                    logger.info(response_dict)
                    continue

            raise Exception("Total number of expected records has changed\nExpected: {}\nRetrieved so far: {}"
                            .format(total_expected_records, len(data)))

        if data:
            # Log which one we're on so we can keep track of how far we are, insert into DB ever 1k lines
            logger.info("Retrieved %s lines of get %s: %s feed, writing next %s to DB",
                        entries_processed, contract_type, award_type, len(data))

            if last_run or specific_params:
                process_and_add(data, contract_type, sess, sub_tier_list, county_by_name, county_by_code,
                                state_code_list, country_list, exec_comp_dict, utcnow, threaded)
            else:
                add_processed_data_list(data, sess)

            logger.info("Successfully inserted %s lines of get %s: %s feed, continuing feed retrieval",
                        len(data), contract_type, award_type)

        # if we got less than the full set of records, we can stop calling the feed
        if len(data) < (MAX_ENTRIES * MAX_REQUESTS_AT_ONCE):
            # ensure we loaded the number of records we expected to, otherwise we'll need to reload
            if entries_processed != total_expected_records:
                raise Exception("Records retrieved != Total expected records\nExpected: {}\nRetrieved: {}"
                                .format(total_expected_records, entries_processed))
            else:
                if 'records_received' not in metrics:
                    metrics['records_received'] = total_expected_records
                else:
                    metrics['records_received'] += total_expected_records
                break
        else:
            data = []

    logger.info("Total entries in %s: %s feed: %s", contract_type, award_type, entries_processed)

    logger.info("Processed %s: %s data", contract_type, award_type)


def get_delete_data(contract_type, now, sess, last_run, start_date=None, end_date=None, metrics=None):
    """ Get data from the delete feed """
    if not metrics:
        metrics = {}
    data = []
    yesterday = now - datetime.timedelta(days=1)
    last_run_date = last_run - relativedelta(days=1)
    params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '
    if start_date and end_date:
        params = 'LAST_MOD_DATE:[' + start_date + ',' + end_date + '] '
        # If we just call deletes, we have to set the date. If we don't provide dates, some other part has to have run
        # already so this is the only place it needs to get set.
        if not metrics['start_date']:
            metrics['start_date'] = start_date
        if not metrics['end_date']:
            metrics['end_date'] = end_date

    base_url = delete_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '"'
    logger.info('Starting delete feed: %s', base_url)

    # retrieve the total count of expected records for this pull
    total_expected_records = get_total_expected_records(base_url)
    logger.info('{} record(s) expected from this feed'.format(total_expected_records))

    processed_deletions = 0
    while True:
        exception_retries = -1
        retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
        request_timeout = 60

        try:
            resp = requests.get(base_url + '&start=' + str(processed_deletions), timeout=request_timeout)
            resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces=FPDS_NAMESPACES)
        except (ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception caught. Sleeping {}s and then retrying with a max wait of {}s...'
                            .format(retry_sleep_times[exception_retries], request_timeout))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                raise e

        # only list the data if there's data to list
        try:
            listed_data = list_data(resp_data['feed']['entry'])
        except KeyError:
            listed_data = []

        if len(listed_data) % SPOT_CHECK_COUNT == 0 and processed_deletions > total_expected_records:
            raise Exception("Total number of expected records has changed\nExpected: {}\nRetrieved so far: {}"
                            .format(total_expected_records, len(processed_deletions)))

        for ld in listed_data:
            data.append(ld)
            processed_deletions += 1

        # Every 100 lines, log which one we're on so we can keep track of how far we are
        if processed_deletions % 100 == 0:
            logger.info("On line %s of %s delete feed", str(processed_deletions), contract_type)

        # if we got less than the full set of records we can stop calling the feed
        if len(listed_data) < 10:
            # ensure we loaded the number of records we expected to, otherwise we'll need to reload
            if processed_deletions != total_expected_records:
                raise Exception("Records retrieved != Total expected records\nExpected: {}\nRetrieved: {}"
                                .format(total_expected_records, len(listed_data)))
            else:
                if 'deletes_received' not in metrics:
                    metrics['deletes_received'] = total_expected_records
                else:
                    metrics['deletes_received'] += total_expected_records
                break
        else:
            listed_data = []

    logger.info("Total entries in %s delete feed: %s", contract_type, str(processed_deletions))

    delete_list = []
    delete_dict = {}
    for value in data:
        # get last modified date
        last_modified = value['content'][contract_type]['transactionInformation']['lastModifiedDate']
        unique_string = process_delete_data(value['content'][contract_type], atom_type=contract_type)

        existing_item = sess.query(DetachedAwardProcurement).\
            filter_by(detached_award_proc_unique=unique_string).one_or_none()

        if existing_item:
            # only add to delete list if the last modified date is later than the existing entry's last modified date
            if last_modified > existing_item.last_modified:
                delete_list.append(existing_item.detached_award_procurement_id)
                delete_dict[existing_item.detached_award_procurement_id] = existing_item.detached_award_proc_unique

    # only need to delete values if there's something to delete
    if delete_list:
        if 'records_deleted' not in metrics:
            metrics['records_deleted'] = len(delete_list)
        else:
            metrics['records_deleted'] += len(delete_list)
        sess.query(DetachedAwardProcurement).\
            filter(DetachedAwardProcurement.detached_award_procurement_id.in_(delete_list)).\
            delete(synchronize_session=False)

    # writing the file
    seconds = int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    file_name = now.strftime('%m-%d-%Y') + "_delete_records_" + contract_type + "_" + str(seconds) + ".csv"
    metrics['deleted_{}_records_file'.format(contract_type).lower()] = file_name
    headers = ["detached_award_procurement_id", "detached_award_proc_unique"]
    if CONFIG_BROKER["use_aws"]:
        s3client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        # add headers
        contents = bytes((",".join(headers) + "\n").encode())
        for key, value in delete_dict.items():
            contents += bytes('{},{}\n'.format(key, value).encode())
        s3client.put_object(Bucket=CONFIG_BROKER['fpds_delete_bucket'], Key=file_name, Body=contents)
    else:
        with CsvLocalWriter(file_name, headers) as writer:
            for key, value in delete_dict.items():
                writer.write([key, value])
            writer.finish_batch()


def create_lookups(sess):
    """ Create the lookups used for FPDS derivations.

        Args:
            sess: connection to database

        Returns:
            Dictionaries of sub tier agencies by code, country names by code, county names by state code + county
            code, county codes by state code + county name, state name by code, and executive compensation data by
            DUNS number
    """

    # get and create list of sub tier agencies
    sub_tiers = sess.query(SubTierAgency).all()
    sub_tier_list = {}

    for sub_tier in sub_tiers:
        sub_tier_list[sub_tier.sub_tier_agency_code] = sub_tier

    # get and create list of country code -> country name mappings.
    countries = sess.query(CountryCode).all()
    country_list = {}

    for country in countries:
        country_list[country.country_code] = country.country_name

    # get and create list of state code -> state name mappings. Prime the county lists with state codes
    county_by_name = {}
    county_by_code = {}
    state_code_list = {}
    state_codes = sess.query(States.state_code, func.upper(States.state_name).label('state_name')).all()

    for state_code in state_codes:
        county_by_name[state_code.state_code] = {}
        county_by_code[state_code.state_code] = {}
        state_code_list[state_code.state_code] = state_code.state_name

    # Fill the county lists with data (code -> name mappings and name -> code mappings)
    county_codes = sess.query(CountyCode.county_number, CountyCode.state_code,
                              func.upper(CountyCode.county_name).label('county_name')).all()

    for county_code in county_codes:
        # we don't want any "(CA)" endings, so strip those
        county_name = county_code.county_name.replace(' (CA)', '').strip()

        # we want all the counties in our by-code lookup because we'd be using this table anyway for derivations
        county_by_code[county_code.state_code][county_code.county_number] = county_name

        # if the county name has only letters/spaces then we want it in our by-name lookup, the rest have the potential
        # to be different from the FPDS feed
        if re.match('^[A-Z\s]+$', county_code.county_name):
            county_by_name[county_code.state_code][county_name] = county_code.county_number

    # get and create list of duns -> exec comp data mappings
    exec_comp_dict = {}
    duns_list = sess.query(DUNS).filter(DUNS.high_comp_officer1_full_na.isnot(None)).all()
    for duns in duns_list:
        exec_comp_dict[duns.awardee_or_recipient_uniqu] = \
            {'officer1_name': duns.high_comp_officer1_full_na, 'officer1_amt': duns.high_comp_officer1_amount,
             'officer2_name': duns.high_comp_officer2_full_na, 'officer2_amt': duns.high_comp_officer2_amount,
             'officer3_name': duns.high_comp_officer3_full_na, 'officer3_amt': duns.high_comp_officer3_amount,
             'officer4_name': duns.high_comp_officer4_full_na, 'officer4_amt': duns.high_comp_officer4_amount,
             'officer5_name': duns.high_comp_officer5_full_na, 'officer5_amt': duns.high_comp_officer5_amount}
    del duns_list

    return sub_tier_list, country_list, state_code_list, county_by_name, county_by_code, exec_comp_dict


def main():
    sess = GlobalDB.db().session

    now = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-a', '--all', help='Clear out the database and get historical data', action='store_true')
    parser.add_argument('-l', '--latest', help='Get by last_mod_date stored in DB', action='store_true')
    parser.add_argument('-d', '--delivery', help='Used in conjunction with -a to indicate delivery order feed',
                        action='store_true')
    parser.add_argument('-o', '--other',
                        help='Used in conjunction with -a to indicate all feeds other than delivery order',
                        action='store_true')
    parser.add_argument('-da', '--dates', help='Used in conjunction with -l to specify dates to gather updates from.'
                                               'Should have 2 arguments, first and last day, formatted YYYY/mm/dd',
                        nargs=2, type=str)
    parser.add_argument('-del', '--delete', help='Used to only run the delete feed. First argument must be "both", '
                                                 '"idv", or "award". The second and third arguments must be the first '
                                                 'and last day to run the feeds for, formatted YYYY/mm/dd',
                        nargs=3, type=str)
    args = parser.parse_args()

    award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
    award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]
    metrics_json = {
        'script_name': 'pull_fpds_data.py',
        'start_time': str(now),
        'records_received': 0,
        'deletes_received': 0,
        'records_deleted': 0,
        'deleted_award_records_file': '',
        'deleted_idv_records_file': '',
        'start_date': '',
        'end_date': ''
    }

    sub_tier_list, country_list, state_code_list, county_by_name, county_by_code, exec_comp_dict = create_lookups(sess)

    if args.all:
        if (not args.delivery and not args.other) or (args.delivery and args.other):
            logger.error("When using the -a flag, please include either -d or -o "
                         "(but not both) to indicate which feeds to read in")
            raise ValueError("When using the -a flag, please include either -d or -o "
                             "(but not both) to indicate which feeds to read in")
        logger.info("Starting at: %s", str(datetime.datetime.now()))

        if args.other:
            for award_type in award_types_idv:
                get_data("IDV", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                         country_list, exec_comp_dict, metrics=metrics_json)
            for award_type in award_types_award:
                if award_type != "Delivery Order":
                    get_data("award", award_type, now, sess, sub_tier_list, county_by_name, county_by_code,
                             state_code_list, country_list, exec_comp_dict, metrics=metrics_json)

        elif args.delivery:
            get_data("award", "Delivery Order", now, sess, sub_tier_list, county_by_name, county_by_code,
                     state_code_list, country_list, exec_comp_dict, metrics=metrics_json)

        last_update = sess.query(FPDSUpdate).one_or_none()

        if last_update:
            sess.query(FPDSUpdate).update({"update_date": now}, synchronize_session=False)
        else:
            sess.add(FPDSUpdate(update_date=now))

        sess.commit()
        logger.info("Ending at: %s", str(datetime.datetime.now()))

    elif args.latest:
        logger.info("Starting at: %s", str(datetime.datetime.now()))

        last_update_obj = sess.query(FPDSUpdate).one_or_none()

        # update_date can't be null because it's being used as the PK for the table, so it can only exist or
        # there are no rows in the table. If there are no rows, act like it's an "add all"
        if not last_update_obj:
            logger.error(
                "No last_update date present, please run the script with the -a flag to generate an initial dataset")
            raise ValueError(
                "No last_update date present, please run the script with the -a flag to generate an initial dataset")
        last_update = last_update_obj.update_date
        start_date = None
        end_date = None

        if args.dates:
            start_date = args.dates[0]
            end_date = args.dates[1]

        for award_type in award_types_idv:
            get_data("IDV", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                     country_list, exec_comp_dict, last_update, start_date=start_date, end_date=end_date,
                     metrics=metrics_json)

        for award_type in award_types_award:
            get_data("award", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                     country_list, exec_comp_dict, last_update, start_date=start_date, end_date=end_date,
                     metrics=metrics_json)

        # We also need to process the delete feed
        get_delete_data("IDV", now, sess, last_update, start_date, end_date, metrics=metrics_json)
        get_delete_data("award", now, sess, last_update, start_date, end_date, metrics=metrics_json)
        if not start_date and not end_date:
            sess.query(FPDSUpdate).update({"update_date": now}, synchronize_session=False)

        sess.commit()
        logger.info("Ending at: %s", str(datetime.datetime.now()))
    elif args.delete:
        del_type = args.delete[0]
        if del_type == 'award':
            del_awards = True
            del_idvs = False
        elif del_type == 'idv':
            del_awards = False
            del_idvs = True
        elif del_type == 'both':
            del_awards = True
            del_idvs = True
        else:
            logger.error("Delete argument must be \"idv\", \"award\", or \"both\"")
            raise ValueError("Delete argument must be \"idv\", \"award\", or \"both\"")

        if del_idvs:
            get_delete_data("IDV", now, sess, now, args.delete[1], args.delete[2], metrics=metrics_json)
        if del_awards:
            get_delete_data("award", now, sess, now, args.delete[1], args.delete[2], metrics=metrics_json)
        sess.commit()
    metrics_json['duration'] = str(datetime.datetime.now() - now)

    with open('pull_fpds_data_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)

    # TODO add a correct start date for "all" so we don't get ALL the data or too little of the data
    # TODO fine-tune indexing


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
