import os
import urllib.request
import boto
import zipfile
import logging
import argparse
import requests
import xmltodict
import numpy as np
import pandas as pd
import csv
import io
import asyncio

import datetime
import time
import re
import threading

from sqlalchemy import func

from dateutil.relativedelta import relativedelta

from requests.packages.urllib3.exceptions import ReadTimeoutError

from dataactcore.logging import configure_logging
from dataactcore.config import CONFIG_BROKER

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import SubTierAgency, CountryCode, States, CountyCode, Zips
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.jobModels import FPDSUpdate

from dataactcore.utils.business_categories import get_business_categories
from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter

feed_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&templateName=1.5.0&q="
delete_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=DELETED&templateName=1.5.0&q="
country_code_map = {'USA': 'US', 'ASM': 'AS', 'GUM': 'GU', 'MNP': 'MP', 'PRI': 'PR', 'VIR': 'VI', 'FSM': 'FM',
                    'MHL': 'MH', 'PLW': 'PW', 'XBK': 'UM', 'XHO': 'UM', 'XJV': 'UM', 'XJA': 'UM', 'XKR': 'UM',
                    'XPL': 'UM', 'XMW': 'UM', 'XWK': 'UM'}

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
        obj['referenced_idv_agency_desc'] = data['referencedIDVID']['agencyID']['@name']
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
        obj['referenced_idv_agency_desc'] = data['referencedIDVID']['agencyID']['@name']
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
            obj[value] = data[key]['@description']
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
            obj[value] = data[key]['@description']
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
            obj[value] = data[key]['@description']
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
        obj['place_of_perform_city_name'] = data['placeOfPerformanceZIPCode']['@city']
    except (KeyError, TypeError):
        obj['place_of_perform_city_name'] = None

    # placeOfPerformanceName
    try:
        obj['place_of_perform_county_na'] = data['placeOfPerformanceZIPCode']['@county']
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
            obj[value] = data['principalPlaceOfPerformance'][key]['@name']
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
            obj[value] = data[key]['@description']
        except (KeyError, TypeError):
            obj[value] = None

    # get country of origin name
    try:
        obj['country_of_product_or_desc'] = data['countryOfOrigin']['@name']
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
            obj[value] = data[key]['@description']
        except (KeyError, TypeError):
            obj[value] = None

    # name values associated with certain values in purchaserInformation
    value_map = {'contractingOfficeAgencyID': 'awarding_sub_tier_agency_n',
                 'contractingOfficeID': 'awarding_office_name',
                 'fundingRequestingAgencyID': 'funding_sub_tier_agency_na',
                 'fundingRequestingOfficeID': 'funding_office_name'}

    for key, value in value_map.items():
        try:
            obj[value] = data[key]['@name']
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
            obj[value] = data[key]['@description']
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
            obj['legal_entity_state_descrip'] = data['vendorLocation']['state']['@name']
        except (KeyError, TypeError):
            obj['legal_entity_state_descrip'] = None

    try:
        obj[key] = extract_text(data['vendorLocation']['state'])
    except (KeyError, TypeError):
        obj[key] = None

    # getting the name associated with the country code
    try:
        obj['legal_entity_country_name'] = data['vendorLocation']['countryCode']['@name']
    except (KeyError, TypeError):
        obj['legal_entity_country_name'] = None

    # vendorOrganizationFactors sub-level
    value_map = {'annualRevenue': 'annual_revenue',
                 'isForeignOwnedAndLocated': 'foreign_owned_and_located',
                 'isLimitedLiabilityCorporation': 'limited_liability_corporat',
                 'isShelteredWorkshop': 'the_ability_one_program',
                 'isSubchapterSCorporation': 'subchapter_s_corporation',
                 'numberOfEmployees': 'number_of_employees',
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


def calculate_remaining_fields(obj, sess, sub_tier_list, county_by_name, county_by_code, state_code_list, country_list):
    """ calculate values that aren't in any feed but can be calculated """
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

    # calculate unique key
    key_list = ['agency_id', 'referenced_idv_agency_iden', 'piid', 'award_modification_amendme', 'parent_award_id',
                'transaction_number']
    unique_string = ""
    for item in key_list:
        if len(unique_string) > 0:
            unique_string += "_"
        try:
            if obj[item]:
                unique_string += obj[item]
            else:
                unique_string += "-none-"
        except KeyError:
            unique_string += "-none-"

    # The order of the unique key is agency_id, referenced_idv_agency_iden, piid, award_modification_amendme,
    # parent_award_id, transaction_number
    obj['detached_award_proc_unique'] = unique_string
    return obj


def process_data(data, sess, atom_type, sub_tier_list, county_by_name, county_by_code, state_code_list, country_list):
    """ process the data coming in """
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
        obj['subcontracting_plan_desc'] = data['preferencePrograms']['subcontractPlan']['@description']
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

    obj = calculate_remaining_fields(obj, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                                     country_list)

    try:
        obj['last_modified'] = data['transactionInformation']['lastModifiedDate']
    except (KeyError, TypeError):
        obj['last_modified'] = None

    try:
        obj['initial_report_date'] = data['transactionInformation']['createdDate']
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

        unique_string += "_"

        try:
            unique_string += extract_text(data['contractID']['referencedIDVID']['agencyID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['contractID']['IDVID']['PIID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['contractID']['IDVID']['modNumber'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        unique_string += "_"

        try:
            unique_string += extract_text(data['contractID']['referencedIDVID']['PIID'])
        except (KeyError, TypeError):
            unique_string += "-none-"

        # transaction_number not in IDV feed, just set it to "-none-"
        unique_string += "_-none-"

    return unique_string


def create_processed_data_list(data, contract_type, sess, sub_tier_list, county_by_name, county_by_code,
                               state_code_list, country_list):
    data_list = []
    for value in data:
        tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                               sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                               county_by_code=county_by_code, state_code_list=state_code_list,
                               country_list=country_list)
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
                    country_list, now, threaded=False):
    """ start the processing for data and add it to the DB """
    if threaded:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                                   sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                                   county_by_code=county_by_code, state_code_list=state_code_list,
                                   country_list=country_list)
            tmp_obj['updated_at'] = now

            insert_statement = insert(DetachedAwardProcurement).values(**tmp_obj).\
                on_conflict_do_update(index_elements=['detached_award_proc_unique'], set_=tmp_obj)
            sess.execute(insert_statement)
    else:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], sess, atom_type=contract_type,
                                   sub_tier_list=sub_tier_list, county_by_name=county_by_name,
                                   county_by_code=county_by_code, state_code_list=state_code_list,
                                   country_list=country_list)

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


def get_data(contract_type, award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
             country_list, last_run=None, threaded=False, start_date=None, end_date=None):
    """ get the data from the atom feed based on contract/award type and the last time the script was run """
    data = []
    yesterday = now - datetime.timedelta(days=1)
    utcnow = datetime.datetime.utcnow()
    # if a date that the script was last successfully run is not provided, get all data
    if not last_run:
        params = 'SIGNED_DATE:[2016/10/01,' + yesterday.strftime('%Y/%m/%d') + '] '
    # if a date that the script was last successfully run is provided, get data since that date
    else:
        last_run_date = last_run - relativedelta(days=1)
        params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '
        if start_date and end_date:
            params = 'LAST_MOD_DATE:[' + start_date + ',' + end_date + '] '

    i = 0

    logger.info('Starting get feed: %s%sCONTRACT_TYPE:"%s" AWARD_TYPE:"%s"', feed_url, params, contract_type.upper(),
                award_type)

    base_url = feed_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '" AWARD_TYPE:"' + \
                                    award_type + '"&start='

    while True:

        def get_with_exception_hand(url_string):

            exception_retries = -1
            retry_sleep_times = [5, 30, 60, 180, 300]

            while exception_retries < len(retry_sleep_times):
                try:
                    resp = requests.get(url_string, timeout=60)
                    break
                except (ConnectionResetError, ReadTimeoutError, requests.exceptions.ConnectionError) as e:
                    exception_retries += 1
                    if exception_retries < len(retry_sleep_times):
                        print('Connection exception caught. Sleeping {}s and then retrying...'.format(
                            retry_sleep_times[exception_retries]))
                        time.sleep(retry_sleep_times[exception_retries])
                    else:
                        print('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                        raise e

            return resp
        # End request.get + exceptions

        async def atom_async_get():
            response_list = []
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    None,
                    get_with_exception_hand,
                    base_url + "&start=" + str(i * 10)
                )
                for i in range(100)
            ]
            for response in await asyncio.gather(*futures):
                response_list.append(response)
                pass
            return response_list
        # End async get requests def

        loop = asyncio.get_event_loop()
        full_response = loop.run_until_complete(atom_async_get())

        for next_resp in full_response:
            resp_data = xmltodict.parse(next_resp.text, process_namespaces=True,
                                        namespaces={'http://www.fpdsng.com/FPDS': None,
                                                    'http://www.w3.org/2005/Atom': None,
                                                    'https://www.fpds.gov/FPDS': None})
            try:
                listed_data = list_data(resp_data['feed']['entry'])
            except KeyError:
                listed_data = []

            if last_run:
                for ld in listed_data:
                    data.append(ld)
                    i += 1
            else:
                data.extend(create_processed_data_list(listed_data, contract_type, sess, sub_tier_list, county_by_name,
                                                       county_by_code, state_code_list, country_list))
                i += len(listed_data)

        if data:
            # Log which one we're on so we can keep track of how far we are, insert into DB ever 1k lines
            logger.info("Retrieved %s lines of get %s: %s feed, writing next %s to DB",
                        i, contract_type, award_type, len(listed_data))

            if last_run:
                process_and_add(data, contract_type, sess, sub_tier_list, county_by_name, county_by_code,
                                state_code_list, country_list, utcnow, threaded)
            else:
                add_processed_data_list(data, sess)

            logger.info("Successfully inserted %s lines of get %s: %s feed, continuing feed retrieval",
                        len(listed_data), contract_type, award_type)

            data = []

        # if we got less than 1000 records, we can stop calling the feed
        if len(listed_data) < 1000:
            break

    logger.info("Total entries in %s: %s feed: %s", contract_type, award_type, str(i))

    logger.info("Processed %s: %s data", contract_type, award_type)


def get_delete_data(contract_type, now, sess, last_run, start_date=None, end_date=None):
    """ Get data from the delete feed """
    data = []
    yesterday = now - datetime.timedelta(days=1)
    last_run_date = last_run - relativedelta(days=1)
    params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '
    if start_date and end_date:
        params = 'LAST_MOD_DATE:[' + start_date + ',' + end_date + '] '

    i = 0
    logger.info('Starting delete feed: %sCONTRACT_TYPE:"%s"', delete_url + params, contract_type.upper())
    while True:
        exception_retries = -1
        retry_sleep_times = [5, 30, 60, 180, 300]

        try:
            resp = requests.get(delete_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '"&start=' + str(i),
                                timeout=60)
            resp_data = xmltodict.parse(resp.text, process_namespaces=True,
                                        namespaces={'http://www.fpdsng.com/FPDS': None,
                                                    'http://www.w3.org/2005/Atom': None,
                                                    'https://www.fpds.gov/FPDS': None})
        except (ConnectionResetError, ReadTimeoutError, requests.exceptions.ConnectionError) as e:
            exception_retries += 1
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception caught. Sleeping {}s and then retrying...'.format(
                    retry_sleep_times[exception_retries]))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                raise e

        # only list the data if there's data to list
        try:
            listed_data = list_data(resp_data['feed']['entry'])
        except KeyError:
            listed_data = []

        for ld in listed_data:
            data.append(ld)
            i += 1

        # Every 100 lines, log which one we're on so we can keep track of how far we are
        if i % 100 == 0:
            logger.info("On line %s of %s delete feed", str(i), contract_type)

        # if we got less than 10 records, we can stop calling the feed
        if len(listed_data) < 10:
            break

    logger.info("Total entries in %s delete feed: %s", contract_type, str(i))

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
        # TODO remove this after the first run
        # else:
        #     delete_dict[unique_string] = unique_string

    # only need to delete values if there's something to delete
    if delete_list:
        sess.query(DetachedAwardProcurement).\
            filter(DetachedAwardProcurement.detached_award_procurement_id.in_(delete_list)).\
            delete(synchronize_session=False)

    # writing the file
    seconds = int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds())
    file_name = now.strftime('%m-%d-%Y') + "_delete_records_" + contract_type + "_" + str(seconds) + ".csv"
    headers = ["detached_award_procurement_id", "detached_award_proc_unique"]
    if CONFIG_BROKER["use_aws"]:
        with CsvS3Writer(CONFIG_BROKER['aws_region'], CONFIG_BROKER['fpds_delete_bucket'], file_name,
                         headers) as writer:
            for key, value in delete_dict.items():
                writer.write([key, value])
            writer.finish_batch()
    else:
        with CsvLocalWriter(file_name, headers) as writer:
            for key, value in delete_dict.items():
                writer.write([key, value])
            writer.finish_batch()


def parse_fpds_file(f, sess, sub_tier_list, naics_dict, filename=None):
    if not filename:
        logger.info("Starting file " + str(f))
        csv_file = 'datafeeds\\' + os.path.splitext(os.path.basename(f))[0]
    else:
        logger.info("Starting file " + str(filename))
        csv_file = 'datafeeds\\' + os.path.splitext(os.path.basename(filename))[0]

    nrows = 0
    with zipfile.ZipFile(f) as zfile:
        with zfile.open(csv_file) as dat_file:
            nrows = len(dat_file.readlines())
            logger.info("File contains %s rows", nrows)

    block_size = 10000
    batches = nrows // block_size
    last_block_size = (nrows % block_size)
    batch = 0
    added_rows = 0

    all_cols = [
        "unique_transaction_id", "transaction_status", "dollarsobligated", "baseandexercisedoptionsvalue",
        "baseandalloptionsvalue", "maj_agency_cat", "mod_agency", "maj_fund_agency_cat", "contractingofficeagencyid",
        "contractingofficeid", "fundingrequestingagencyid", "fundingrequestingofficeid", "fundedbyforeignentity",
        "signeddate", "effectivedate", "currentcompletiondate", "ultimatecompletiondate", "lastdatetoorder",
        "contractactiontype", "reasonformodification", "typeofcontractpricing", "priceevaluationpercentdifference",
        "subcontractplan", "lettercontract", "multiyearcontract", "performancebasedservicecontract", "majorprogramcode",
        "contingencyhumanitarianpeacekeepingoperation", "contractfinancing", "costorpricingdata",
        "costaccountingstandardsclause", "descriptionofcontractrequirement", "purchasecardaspaymentmethod",
        "numberofactions", "nationalinterestactioncode", "progsourceagency", "progsourceaccount", "progsourcesubacct",
        "account_title", "rec_flag", "typeofidc", "multipleorsingleawardidc", "programacronym", "vendorname",
        "vendoralternatename", "vendorlegalorganizationname", "vendordoingasbusinessname", "divisionname",
        "divisionnumberorofficecode", "vendorenabled", "vendorlocationdisableflag", "ccrexception", "streetaddress",
        "streetaddress2", "streetaddress3", "city", "state", "zipcode", "vendorcountrycode", "vendor_state_code",
        "vendor_cd", "congressionaldistrict", "vendorsitecode", "vendoralternatesitecode", "dunsnumber",
        "parentdunsnumber", "phoneno", "faxno", "registrationdate", "renewaldate", "mod_parent", "locationcode",
        "statecode", "PlaceofPerformanceCity", "pop_state_code", "placeofperformancecountrycode",
        "placeofperformancezipcode", "pop_cd", "placeofperformancecongressionaldistrict", "psc_cat",
        "productorservicecode", "systemequipmentcode", "claimantprogramcode", "principalnaicscode",
        "informationtechnologycommercialitemcategory", "gfe_gfp", "useofepadesignatedproducts",
        "recoveredmaterialclauses", "seatransportation", "contractbundling", "consolidatedcontract", "countryoforigin",
        "placeofmanufacture", "manufacturingorganizationtype", "agencyid", "piid", "modnumber", "transactionnumber",
        "fiscal_year", "idvagencyid", "idvpiid", "idvmodificationnumber", "solicitationid", "extentcompeted",
        "reasonnotcompeted", "numberofoffersreceived", "commercialitemacquisitionprocedures",
        "commercialitemtestprogram", "smallbusinesscompetitivenessdemonstrationprogram", "a76action",
        "competitiveprocedures", "solicitationprocedures", "typeofsetaside", "localareasetaside", "evaluatedpreference",
        "fedbizopps", "research", "statutoryexceptiontofairopportunity", "organizationaltype", "numberofemployees",
        "annualrevenue", "firm8aflag", "hubzoneflag", "sdbflag", "issbacertifiedsmalldisadvantagedbusiness",
        "shelteredworkshopflag", "hbcuflag", "educationalinstitutionflag", "womenownedflag", "veteranownedflag",
        "srdvobflag", "localgovernmentflag", "minorityinstitutionflag", "aiobflag", "stategovernmentflag",
        "federalgovernmentflag", "minorityownedbusinessflag", "apaobflag", "tribalgovernmentflag", "baobflag",
        "naobflag", "saaobflag", "nonprofitorganizationflag", "isothernotforprofitorganization",
        "isforprofitorganization", "isfoundation", "haobflag", "ishispanicservicinginstitution",
        "emergingsmallbusinessflag", "hospitalflag", "contractingofficerbusinesssizedetermination",
        "is1862landgrantcollege", "is1890landgrantcollege", "is1994landgrantcollege", "isveterinarycollege",
        "isveterinaryhospital", "isprivateuniversityorcollege", "isschoolofforestry",
        "isstatecontrolledinstitutionofhigherlearning", "isserviceprovider", "receivescontracts", "receivesgrants",
        "receivescontractsandgrants", "isairportauthority", "iscouncilofgovernments",
        "ishousingauthoritiespublicortribal", "isinterstateentity", "isplanningcommission", "isportauthority",
        "istransitauthority", "issubchapterscorporation", "islimitedliabilitycorporation", "isforeignownedandlocated",
        "isarchitectureandengineering", "isdotcertifieddisadvantagedbusinessenterprise", "iscitylocalgovernment",
        "iscommunitydevelopedcorporationownedfirm", "iscommunitydevelopmentcorporation", "isconstructionfirm",
        "ismanufacturerofgoods", "iscorporateentitynottaxexempt", "iscountylocalgovernment", "isdomesticshelter",
        "isfederalgovernmentagency", "isfederallyfundedresearchanddevelopmentcorp", "isforeigngovernment",
        "isindiantribe", "isintermunicipallocalgovernment", "isinternationalorganization", "islaborsurplusareafirm",
        "islocalgovernmentowned", "ismunicipalitylocalgovernment", "isnativehawaiianownedorganizationorfirm",
        "isotherbusinessororganization", "isotherminorityowned", "ispartnershiporlimitedliabilitypartnership",
        "isschooldistrictlocalgovernment", "issmallagriculturalcooperative", "issoleproprietorship",
        "istownshiplocalgovernment", "istriballyownedfirm", "istribalcollege", "isalaskannativeownedcorporationorfirm",
        "iscorporateentitytaxexempt", "iswomenownedsmallbusiness", "isecondisadvwomenownedsmallbusiness",
        "isjointventurewomenownedsmallbusiness", "isjointventureecondisadvwomenownedsmallbusiness", "walshhealyact",
        "servicecontractact", "davisbaconact", "clingercohenact", "otherstatutoryauthority", "prime_awardee_executive1",
        "prime_awardee_executive1_compensation", "prime_awardee_executive2", "prime_awardee_executive2_compensation",
        "prime_awardee_executive3", "prime_awardee_executive3_compensation", "prime_awardee_executive4",
        "prime_awardee_executive4_compensation", "prime_awardee_executive5", "prime_awardee_executive5_compensation",
        "interagencycontractingauthority", "last_modified_date"]

    while batch <= batches:
        skiprows = 1 if batch == 0 else (batch * block_size)
        nrows = (((batch + 1) * block_size) - skiprows) if (batch < batches) else last_block_size
        logger.info('Starting load for rows %s to %s', skiprows + 1, nrows + skiprows)

        with zipfile.ZipFile(f) as zfile:
            with zfile.open(csv_file) as dat_file:
                data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows, names=all_cols)

                cdata = format_fpds_data(data, sub_tier_list, naics_dict)
                if cdata is not None:
                    logger.info("Loading {} rows into database".format(len(cdata.index)))

                    try:
                        insert_dataframe(cdata, DetachedAwardProcurement.__table__.name, sess.connection())
                        sess.commit()
                    except IntegrityError:
                        sess.rollback()
                        logger.info("Bulk load failed, individually loading %s rows into database", len(cdata.index))
                        for index, row in cdata.iterrows():
                            try:
                                statement = insert(DetachedAwardProcurement).values(**row)
                                sess.execute(statement)
                                sess.commit()
                            except IntegrityError:
                                sess.rollback()
                                logger.info("Found duplicate: %s, row not inserted", row['detached_award_proc_unique'])

        added_rows += nrows
        batch += 1
    logger.info("Finished loading file")


def format_fpds_data(data, sub_tier_list, naics_data):
    logger.info("Formatting data")

    if len(data.index) == 0:
        return None

    # drop all columns we don't want
    bad_cols = [
        'unique_transaction_id', 'maj_agency_cat', 'mod_agency', 'maj_fund_agency_cat', 'progsourceagency',
        'progsourceaccount', 'progsourcesubacct', 'account_title', 'rec_flag', 'state', 'congressionaldistrict',
        'registrationdate', 'renewaldate', 'statecode', 'placeofperformancecongressionaldistrict', 'psc_cat',
        'fiscal_year', 'competitiveprocedures', 'organizationaltype', 'isserviceprovider', 'locationcode',
        'isarchitectureandengineering', 'isconstructionfirm', 'isotherbusinessororganization',
        'prime_awardee_executive1', 'prime_awardee_executive1_compensation', 'prime_awardee_executive2',
        'prime_awardee_executive2_compensation', 'prime_awardee_executive3', 'prime_awardee_executive3_compensation',
        'prime_awardee_executive4', 'prime_awardee_executive4_compensation', 'prime_awardee_executive5',
        'prime_awardee_executive5_compensation']
    for tag in bad_cols:
        del data[tag]

    # drop rows with transaction_status not active, drop transaction_status column when done
    data = data[data['transaction_status'] == "active"].copy()
    del data['transaction_status']

    logger.info('Starting splitting columns')
    # mappings to split the columns that have the tag and description in the same entry into 2
    colon_split_mappings = {
        'claimantprogramcode': 'dod_claimant_prog_cod_desc',
        'commercialitemacquisitionprocedures': 'commercial_item_acqui_desc',
        'commercialitemtestprogram': 'commercial_item_test_desc',
        'consolidatedcontract': 'consolidated_contract_desc',
        'contingencyhumanitarianpeacekeepingoperation': 'contingency_humanitar_desc',
        'contractbundling': 'contract_bundling_descrip',
        'contractfinancing': 'contract_financing_descrip',
        'contractingofficeagencyid': 'awarding_sub_tier_agency_n',
        'contractingofficeid': 'awarding_office_name',
        'contractingofficerbusinesssizedetermination': 'contracting_officers_desc',
        'costorpricingdata': 'cost_or_pricing_data_desc',
        'countryoforigin': 'country_of_product_or_desc',
        'evaluatedpreference': 'evaluated_preference_desc',
        'extentcompeted': 'extent_compete_description',
        'fundingrequestingagencyid': 'funding_sub_tier_agency_na',
        'fundingrequestingofficeid': 'funding_office_name',
        'gfe_gfp': 'government_furnished_desc',
        'informationtechnologycommercialitemcategory': 'information_technolog_desc',
        'interagencycontractingauthority': 'interagency_contract_desc',
        'manufacturingorganizationtype': 'domestic_or_foreign_e_desc',
        'multipleorsingleawardidc': 'multiple_or_single_aw_desc',
        'multiyearcontract': 'multi_year_contract_desc',
        'nationalinterestactioncode': 'national_interest_desc',
        'performancebasedservicecontract': 'performance_based_se_desc',
        'placeofmanufacture': 'place_of_manufacture_desc',
        'placeofperformancecountrycode': 'place_of_perf_country_desc',
        'pop_state_code': 'place_of_perfor_state_desc',
        'productorservicecode': 'product_or_service_co_desc',
        'purchasecardaspaymentmethod': 'purchase_card_as_paym_desc',
        'reasonformodification': 'action_type_description',
        'reasonnotcompeted': 'other_than_full_and_o_desc',
        'recoveredmaterialclauses': 'recovered_materials_s_desc',
        'seatransportation': 'sea_transportation_desc',
        'solicitationprocedures': 'solicitation_procedur_desc',
        'subcontractplan': 'subcontracting_plan_desc',
        'systemequipmentcode': 'program_system_or_equ_desc',
        'typeofcontractpricing': 'type_of_contract_pric_desc',
        'typeofsetaside': 'type_set_aside_description',
        'useofepadesignatedproducts': 'epa_designated_produc_desc',
        'vendorcountrycode': 'legal_entity_country_name',
        'walshhealyact': 'materials_supplies_descrip'
    }
    for tag, description in colon_split_mappings.items():
        data[description] = data.apply(lambda x: get_data_after_colon(x, tag), axis=1)
        data[tag] = data.apply(lambda x: get_data_before_colon(x, tag), axis=1)

    logger.info('Starting manually mapping columns')
    # mappings for manual description entry
    manual_description_mappings = {
        'ccrexception': 'sam_exception_description',
        'costaccountingstandardsclause': 'cost_accounting_stand_desc',
        'davisbaconact': 'construction_wage_rat_desc',
        'fedbizopps': 'fed_biz_opps_description',
        'fundedbyforeignentity': 'foreign_funding_desc',
        'lettercontract': 'undefinitized_action_desc',
        'research': 'research_description',
        'servicecontractact': 'labor_standards_descrip',
        'statutoryexceptiontofairopportunity': 'fair_opportunity_limi_desc',
        'typeofidc': 'type_of_idc_description',
    }
    type_to_description = {
        'ccrexception': {
            '1': 'GOVERNMENT - WIDE COMMERCIAL PURCHASE CARD',
            '2': 'CLASSIFIED CONTRACTS',
            '3': 'CONTRACTING OFFICERS DEPLOYED IN THE COURSE OF MILITARY OPERATIONS',
            '4': 'CONTRACTING OFFICERS CONDUCTING EMERGENCY OPERATIONS',
            '5': 'CONTRACTS TO SUPPORT UNUSUAL OR COMPELLING NEEDS',
            '6': 'AWARDS TO FOREIGN VENDORS FOR WORK PERFORMED OUTSIDE THE UNITED STATES',
            '7': 'MICRO-PURCHASES THAT DO NOT USE THE EFT'
        },
        'costaccountingstandardsclause': {
            'Y': 'YES - CAS CLAUSE INCLUDED',
            'N': 'NO - CAS WAIVER APPROVED',
            'X': 'NOT APPLICABLE EXEMPT FROM CAS'
        },
        'davisbaconact': {
            'Y': 'YES',
            'N': 'NO',
            'X': 'NOT APPLICABLE'
        },
        'fedbizopps': {
            'Y': 'YES',
            'N': 'NO',
            'X': 'NOT APPLICABLE'
        },
        'fundedbyforeignentity': {
            'A': 'FOREIGN FUNDS FMS',
            'B': 'FOREIGN FUNDS NON-FMS',
            'X': 'NOT APPLICABLE'
        },
        'lettercontract': {
            'A': 'LETTER CONTRACT',
            'B': 'OTHER UNDEFINITIZED ACTION',
            'X': 'NO'
        },
        'research': {
            'SR1': 'SMALL BUSINESS INNOVATION RESEARCH PROGRAM PHASE I ACTION',
            'SR2': 'SMALL BUSINESS INNOVATION RESEARCH PROGRAM PHASE II ACTION',
            'SR3': 'SMALL BUSINESS INNOVATION RESEARCH PROGRAM PHASE III ACTION',
            'ST1': 'SMALL TECHNOLOGY TRANSFER RESEARCH PROGRAM PHASE I',
            'ST2': 'SMALL TECHNOLOGY TRANSFER RESEARCH PROGRAM PHASE II',
            'ST3': 'SMALL TECHNOLOGY TRANSFER RESEARCH PROGRAM PHASE III'
        },
        'servicecontractact': {
            'Y': 'YES',
            'N': 'NO',
            'X': 'NOT APPLICABLE'
        },
        'statutoryexceptiontofairopportunity': {
            'URG': 'URGENCY',
            'ONE': 'ONLY ONE SOURCE - OTHER',
            'FOO': 'FOLLOW-ON ACTION FOLLOWING COMPETITIVE INITIAL ACTION',
            'MG': 'MINIMUM GUARANTEE',
            'OSA': 'OTHER STATUTORY AUTHORITY',
            'FAIR': 'FAIR OPPORTUNITY GIVEN',
            'CSA': 'COMPETITIVE SET ASIDE',
            'SS': 'SOLE SOURCE'
        },
        'typeofidc': {
            'A': 'INDEFINITE DELIVERY / REQUIREMENTS',
            'B': 'INDEFINITE DELIVERY / INDEFINITE QUANTITY',
            'C': 'INDEFINITE DELIVERY / DEFINITE QUANTITY'
        },
    }
    for tag, description in manual_description_mappings.items():
        data[description] = data.apply(lambda x: map_description_manual(x, tag, type_to_description[tag]), axis=1)
        data[tag] = data.apply(lambda x: map_type_manual(x, tag, type_to_description[tag]), axis=1)

    logger.info('Starting pre-colon data gathering')
    # clean up a couple other tags that just need the tag in the data
    tag_only = ['agencyid', 'smallbusinesscompetitivenessdemonstrationprogram', 'principalnaicscode']
    for tag in tag_only:
        data[tag] = data.apply(lambda x: get_data_before_colon(x, tag), axis=1)

    logger.info('Starting specialized mappings')
    # map legal_entity_state data depending on given conditions then drop vendor_state_code since it's been split now
    data['legal_entity_state_code'] = data.apply(lambda x: map_legal_entity_state_code(x), axis=1)
    data['legal_entity_state_descrip'] = data.apply(lambda x: map_legal_entity_state_descrip(x), axis=1)
    del data['vendor_state_code']

    # map contents of contractactiontype to relevant columns then delete contractactiontype column
    award_contract_type_mappings = {
        'BPA Call Blanket Purchase Agreement': 'A', 'PO Purchase Order': 'B',
        'DO Delivery Order': 'C', 'DCA Definitive Contract': 'D'
    }
    award_contract_desc_mappings = {
        'BPA Call Blanket Purchase Agreement': 'BPA CALL', 'PO Purchase Order': 'PURCHASE ORDER',
        'DO Delivery Order': 'DELIVERY ORDER', 'DCA Definitive Contract': 'DEFINITIVE CONTRACT'
    }
    idv_type_mappings = {
        'GWAC Government Wide Acquisition Contract': 'A',
        'IDC Indefinite Delivery Contract': 'B',
        'FSS Federal Supply Schedule': 'C',
        'BOA Basic Ordering Agreement': 'D',
        'BPA Blanket Purchase Agreement': 'E'
    }
    idv_desc_mappings = {
        'GWAC Government Wide Acquisition Contract': 'GWAC',
        'IDC Indefinite Delivery Contract': 'IDC',
        'FSS Federal Supply Schedule': 'FSS',
        'BOA Basic Ordering Agreement': 'BOA',
        'BPA Blanket Purchase Agreement': 'BPA'
    }
    data['contract_award_type'] = data.apply(lambda x: map_type(x, award_contract_type_mappings), axis=1)
    data['contract_award_type_desc'] = data.apply(lambda x: map_type_description(x, award_contract_desc_mappings),
                                                  axis=1)
    data['idv_type'] = data.apply(lambda x: map_type(x, idv_type_mappings), axis=1)
    data['idv_type_description'] = data.apply(lambda x: map_type_description(x, idv_desc_mappings), axis=1)
    data['pulled_from'] = data.apply(lambda x: map_pulled_from(x, award_contract_type_mappings, idv_type_mappings),
                                     axis=1)
    del data['contractactiontype']

    logger.info('Starting date formatting and null filling')
    # formatting dates for relevant columns
    date_format_list = ['currentcompletiondate', 'effectivedate', 'last_modified_date', 'lastdatetoorder', 'signeddate',
                        'ultimatecompletiondate']
    for col in date_format_list:
        data[col] = data.apply(lambda x: format_date(x, col), axis=1)

    # adding columns missing from historical data
    null_list = [
        'a_76_fair_act_action_desc', 'alaskan_native_servicing_i', 'clinger_cohen_act_pla_desc', 'initial_report_date',
        'local_area_set_aside_desc', 'native_hawaiian_servicing', 'place_of_perform_county_na', 'referenced_idv_type',
        'referenced_idv_type_desc', 'referenced_mult_or_single', 'referenced_mult_or_si_desc',
        'sba_certified_8_a_joint_ve', 'us_government_entity'
    ]
    for item in null_list:
        data[item] = None

    logger.info('Starting cgac/naics and unique key derivations')
    # map using cgac codes
    data['awarding_agency_code'] = data.apply(lambda x: map_agency_code(x, 'contractingofficeagencyid', sub_tier_list),
                                              axis=1)
    data['awarding_agency_name'] = data.apply(lambda x: map_agency_name(x, 'contractingofficeagencyid', sub_tier_list),
                                              axis=1)
    data['funding_agency_code'] = data.apply(lambda x: map_agency_code(x, 'fundingrequestingagencyid', sub_tier_list),
                                             axis=1)
    data['funding_agency_name'] = data.apply(lambda x: map_agency_name(x, 'fundingrequestingagencyid', sub_tier_list),
                                             axis=1)
    data['referenced_idv_agency_desc'] = data.apply(lambda x: map_sub_tier_name(x, 'idvagencyid', sub_tier_list),
                                                    axis=1)

    # map naics codes
    data['naics_description'] = data.apply(lambda x: map_naics(x, 'principalnaicscode', naics_data), axis=1)

    # create the unique key
    data['detached_award_proc_unique'] = data.apply(lambda x: create_unique_key(x), axis=1)

    logger.info('Cleaning data and fixing np.nan to None')
    # clean the data
    cdata = clean_data(
        data,
        DetachedAwardProcurement,
        {
            'a_76_fair_act_action_desc': 'a_76_fair_act_action_desc',
            'a76action': 'a_76_fair_act_action',
            'action_type_description': 'action_type_description',
            'agencyid': 'agency_id',
            'aiobflag': 'american_indian_owned_busi',
            'alaskan_native_servicing_i': 'alaskan_native_servicing_i',
            'annualrevenue': 'annual_revenue',
            'apaobflag': 'asian_pacific_american_own',
            'awarding_agency_code': 'awarding_agency_code',
            'awarding_agency_name': 'awarding_agency_name',
            'awarding_office_name': 'awarding_office_name',
            'awarding_sub_tier_agency_n': 'awarding_sub_tier_agency_n',
            'baobflag': 'black_american_owned_busin',
            'baseandexercisedoptionsvalue': 'base_exercised_options_val',
            'baseandalloptionsvalue': 'base_and_all_options_value',
            'ccrexception': 'sam_exception',
            'city': 'legal_entity_city_name',
            'claimantprogramcode': 'dod_claimant_program_code',
            'clinger_cohen_act_pla_desc': 'clinger_cohen_act_pla_desc',
            'clingercohenact': 'clinger_cohen_act_planning',
            'commercial_item_acqui_desc': 'commercial_item_acqui_desc',
            'commercial_item_test_desc': 'commercial_item_test_desc',
            'commercialitemacquisitionprocedures': 'commercial_item_acquisitio',
            'commercialitemtestprogram': 'commercial_item_test_progr',
            'consolidated_contract_desc': 'consolidated_contract_desc',
            'consolidatedcontract': 'consolidated_contract',
            'contingency_humanitar_desc': 'contingency_humanitar_desc',
            'contingencyhumanitarianpeacekeepingoperation': 'contingency_humanitarian_o',
            'contract_award_type': 'contract_award_type',
            'contract_award_type_desc': 'contract_award_type_desc',
            'contract_bundling_descrip': 'contract_bundling_descrip',
            'contract_financing_descrip': 'contract_financing_descrip',
            'contractbundling': 'contract_bundling',
            'contractfinancing': 'contract_financing',
            'contracting_officers_desc': 'contracting_officers_desc',
            'contractingofficeagencyid': 'awarding_sub_tier_agency_c',
            'contractingofficeid': 'awarding_office_code',
            'contractingofficerbusinesssizedetermination': 'contracting_officers_deter',
            'cost_accounting_stand_desc': 'cost_accounting_stand_desc',
            'cost_or_pricing_data_desc': 'cost_or_pricing_data_desc',
            'costaccountingstandardsclause': 'cost_accounting_standards',
            'costorpricingdata': 'cost_or_pricing_data',
            'country_of_product_or_desc': 'country_of_product_or_desc',
            'countryoforigin': 'country_of_product_or_serv',
            'currentcompletiondate': 'period_of_performance_curr',
            'detached_award_proc_unique': 'detached_award_proc_unique',
            'construction_wage_rat_desc': 'construction_wage_rat_desc',
            'davisbaconact': 'construction_wage_rate_req',
            'descriptionofcontractrequirement': 'award_description',
            'divisionname': 'division_name',
            'divisionnumberorofficecode': 'division_number_or_office',
            'dod_claimant_prog_cod_desc': 'dod_claimant_prog_cod_desc',
            'dollarsobligated': 'federal_action_obligation',
            'domestic_or_foreign_e_desc': 'domestic_or_foreign_e_desc',
            'dunsnumber': 'awardee_or_recipient_uniqu',
            'educationalinstitutionflag': 'educational_institution',
            'effectivedate': 'period_of_performance_star',
            'emergingsmallbusinessflag': 'emerging_small_business',
            'epa_designated_produc_desc': 'epa_designated_produc_desc',
            'evaluated_preference_desc': 'evaluated_preference_desc',
            'evaluatedpreference': 'evaluated_preference',
            'extent_compete_description': 'extent_compete_description',
            'extentcompeted': 'extent_competed',
            'fair_opportunity_limi_desc': 'fair_opportunity_limi_desc',
            'faxno': 'vendor_fax_number',
            'fed_biz_opps_description': 'fed_biz_opps_description',
            'fedbizopps': 'fed_biz_opps',
            'federalgovernmentflag': 'us_federal_government',
            'firm8aflag': 'c8a_program_participant',
            'foreign_funding_desc': 'foreign_funding_desc',
            'fundedbyforeignentity': 'foreign_funding',
            'funding_agency_code': 'funding_agency_code',
            'funding_agency_name': 'funding_agency_name',
            'funding_office_name': 'funding_office_name',
            'funding_sub_tier_agency_na': 'funding_sub_tier_agency_na',
            'fundingrequestingagencyid': 'funding_sub_tier_agency_co',
            'fundingrequestingofficeid': 'funding_office_code',
            'gfe_gfp': 'government_furnished_prope',
            'government_furnished_desc': 'government_furnished_desc',
            'haobflag': 'hispanic_american_owned_bu',
            'hbcuflag': 'historically_black_college',
            'hospitalflag': 'hospital_flag',
            'hubzoneflag': 'historically_underutilized',
            'idv_type': 'idv_type',
            'idv_type_description': 'idv_type_description',
            'idvagencyid': 'referenced_idv_agency_iden',
            'idvmodificationnumber': 'referenced_idv_modificatio',
            'idvpiid': 'parent_award_id',
            'information_technolog_desc': 'information_technolog_desc',
            'informationtechnologycommercialitemcategory': 'information_technology_com',
            'initial_report_date': 'initial_report_date',
            'interagency_contract_desc': 'interagency_contract_desc',
            'interagencycontractingauthority': 'interagency_contracting_au',
            'is1862landgrantcollege': 'c1862_land_grant_college',
            'is1890landgrantcollege': 'c1890_land_grant_college',
            'is1994landgrantcollege': 'c1994_land_grant_college',
            'isairportauthority': 'airport_authority',
            'isalaskannativeownedcorporationorfirm': 'alaskan_native_owned_corpo',
            'iscitylocalgovernment': 'city_local_government',
            'iscommunitydevelopedcorporationownedfirm': 'community_developed_corpor',
            'iscommunitydevelopmentcorporation': 'community_development_corp',
            'iscorporateentitynottaxexempt': 'corporate_entity_not_tax_e',
            'iscorporateentitytaxexempt': 'corporate_entity_tax_exemp',
            'iscouncilofgovernments': 'council_of_governments',
            'iscountylocalgovernment': 'county_local_government',
            'isdomesticshelter': 'domestic_shelter',
            'isdotcertifieddisadvantagedbusinessenterprise': 'dot_certified_disadvantage',
            'isecondisadvwomenownedsmallbusiness': 'economically_disadvantaged',
            'isfederalgovernmentagency': 'federal_agency',
            'isfederallyfundedresearchanddevelopmentcorp': 'federally_funded_research',
            'isforeigngovernment': 'foreign_government',
            'isforeignownedandlocated': 'foreign_owned_and_located',
            'isforprofitorganization': 'for_profit_organization',
            'isfoundation': 'foundation',
            'ishispanicservicinginstitution': 'hispanic_servicing_institu',
            'ishousingauthoritiespublicortribal': 'housing_authorities_public',
            'isindiantribe': 'indian_tribe_federally_rec',
            'isintermunicipallocalgovernment': 'inter_municipal_local_gove',
            'isinternationalorganization': 'international_organization',
            'isinterstateentity': 'interstate_entity',
            'isjointventureecondisadvwomenownedsmallbusiness': 'joint_venture_economically',
            'isjointventurewomenownedsmallbusiness': 'joint_venture_women_owned',
            'islaborsurplusareafirm': 'labor_surplus_area_firm',
            'islimitedliabilitycorporation': 'limited_liability_corporat',
            'islocalgovernmentowned': 'local_government_owned',
            'ismanufacturerofgoods': 'manufacturer_of_goods',
            'ismunicipalitylocalgovernment': 'municipality_local_governm',
            'isnativehawaiianownedorganizationorfirm': 'native_hawaiian_owned_busi',
            'isotherminorityowned': 'other_minority_owned_busin',
            'isothernotforprofitorganization': 'other_not_for_profit_organ',
            'ispartnershiporlimitedliabilitypartnership': 'partnership_or_limited_lia',
            'isplanningcommission': 'planning_commission',
            'isportauthority': 'port_authority',
            'isprivateuniversityorcollege': 'private_university_or_coll',
            'issbacertifiedsmalldisadvantagedbusiness': 'small_disadvantaged_busine',
            'isschooldistrictlocalgovernment': 'school_district_local_gove',
            'isschoolofforestry': 'school_of_forestry',
            'issmallagriculturalcooperative': 'small_agricultural_coopera',
            'issoleproprietorship': 'sole_proprietorship',
            'isstatecontrolledinstitutionofhigherlearning': 'state_controlled_instituti',
            'issubchapterscorporation': 'subchapter_s_corporation',
            'istownshiplocalgovernment': 'township_local_government',
            'istransitauthority': 'transit_authority',
            'istribalcollege': 'tribal_college',
            'istriballyownedfirm': 'tribally_owned_business',
            'isveterinarycollege': 'veterinary_college',
            'isveterinaryhospital': 'veterinary_hospital',
            'iswomenownedsmallbusiness': 'women_owned_small_business',
            'lastdatetoorder': 'ordering_period_end_date',
            'last_modified_date': 'last_modified',
            'legal_entity_country_name': 'legal_entity_country_name',
            'legal_entity_state_code': 'legal_entity_state_code',
            'legal_entity_state_descrip': 'legal_entity_state_descrip',
            'lettercontract': 'undefinitized_action',
            'local_area_set_aside_desc': 'local_area_set_aside_desc',
            'localareasetaside': 'local_area_set_aside',
            'localgovernmentflag': 'us_local_government',
            'majorprogramcode': 'major_program',
            'manufacturingorganizationtype': 'domestic_or_foreign_entity',
            'minorityinstitutionflag': 'minority_institution',
            'minorityownedbusinessflag': 'minority_owned_business',
            'mod_parent': 'ultimate_parent_legal_enti',
            'modnumber': 'award_modification_amendme',
            'multiple_or_single_aw_desc': 'multiple_or_single_aw_desc',
            'multipleorsingleawardidc': 'multiple_or_single_award_i',
            'multi_year_contract_desc': 'multi_year_contract_desc',
            'multiyearcontract': 'multi_year_contract',
            'naics_description': 'naics_description',
            'naobflag': 'native_american_owned_busi',
            'national_interest_desc': 'national_interest_desc',
            'nationalinterestactioncode': 'national_interest_action',
            'native_hawaiian_servicing': 'native_hawaiian_servicing',
            'nonprofitorganizationflag': 'nonprofit_organization',
            'numberofactions': 'number_of_actions',
            'numberofemployees': 'number_of_employees',
            'numberofoffersreceived': 'number_of_offers_received',
            'other_than_full_and_o_desc': 'other_than_full_and_o_desc',
            'otherstatutoryauthority': 'other_statutory_authority',
            'parentdunsnumber': 'ultimate_parent_unique_ide',
            'performance_based_se_desc': 'performance_based_se_desc',
            'performancebasedservicecontract': 'performance_based_service',
            'phoneno': 'vendor_phone_number',
            'piid': 'piid',
            'place_of_manufacture_desc': 'place_of_manufacture_desc',
            'place_of_perf_country_desc': 'place_of_perf_country_desc',
            'place_of_perfor_state_desc': 'place_of_perfor_state_desc',
            'place_of_perform_county_na': 'place_of_perform_county_na',
            'placeofmanufacture': 'place_of_manufacture',
            'placeofperformancecity': 'place_of_perform_city_name',
            'placeofperformancecountrycode': 'place_of_perform_country_c',
            'placeofperformancezipcode': 'place_of_performance_zip4a',
            'pop_cd': 'place_of_performance_congr',
            'pop_state_code': 'place_of_performance_state',
            'priceevaluationpercentdifference': 'price_evaluation_adjustmen',
            'principalnaicscode': 'naics',
            'product_or_service_co_desc': 'product_or_service_co_desc',
            'productorservicecode': 'product_or_service_code',
            'program_system_or_equ_desc': 'program_system_or_equ_desc',
            'programacronym': 'program_acronym',
            'pulled_from': 'pulled_from',
            'purchase_card_as_paym_desc': 'purchase_card_as_paym_desc',
            'purchasecardaspaymentmethod': 'purchase_card_as_payment_m',
            'reasonformodification': 'action_type',
            'reasonnotcompeted': 'other_than_full_and_open_c',
            'receivescontracts': 'contracts',
            'receivescontractsandgrants': 'receives_contracts_and_gra',
            'receivesgrants': 'grants',
            'recovered_materials_s_desc': 'recovered_materials_s_desc',
            'recoveredmaterialclauses': 'recovered_materials_sustai',
            'referenced_idv_agency_desc': 'referenced_idv_agency_desc',
            'referenced_idv_type': 'referenced_idv_type',
            'referenced_idv_type_desc': 'referenced_idv_type_desc',
            'referenced_mult_or_single': 'referenced_mult_or_single',
            'referenced_mult_or_si_desc': 'referenced_mult_or_si_desc',
            'research': 'research',
            'research_description': 'research_description',
            'saaobflag': 'subcontinent_asian_asian_i',
            'sam_exception_description': 'sam_exception_description',
            'sba_certified_8_a_joint_ve': 'sba_certified_8_a_joint_ve',
            'sdbflag': 'self_certified_small_disad',
            'sea_transportation_desc': 'sea_transportation_desc',
            'seatransportation': 'sea_transportation',
            'labor_standards_descrip': 'labor_standards_descrip',
            'servicecontractact': 'labor_standards',
            'signeddate': 'action_date',
            'shelteredworkshopflag': 'the_ability_one_program',
            'smallbusinesscompetitivenessdemonstrationprogram': 'small_business_competitive',
            'solicitation_procedur_desc': 'solicitation_procedur_desc',
            'solicitationid': 'solicitation_identifier',
            'solicitationprocedures': 'solicitation_procedures',
            'stategovernmentflag': 'us_state_government',
            'statutoryexceptiontofairopportunity': 'fair_opportunity_limited_s',
            'srdvobflag': 'service_disabled_veteran_o',
            'streetaddress': 'legal_entity_address_line1',
            'streetaddress2': 'legal_entity_address_line2',
            'streetaddress3': 'legal_entity_address_line3',
            'subcontracting_plan_desc': 'subcontracting_plan_desc',
            'subcontractplan': 'subcontracting_plan',
            'systemequipmentcode': 'program_system_or_equipmen',
            'transactionnumber': 'transaction_number',
            'tribalgovernmentflag': 'us_tribal_government',
            'type_of_contract_pric_desc': 'type_of_contract_pric_desc',
            'type_of_idc_description': 'type_of_idc_description',
            'type_set_aside_description': 'type_set_aside_description',
            'typeofcontractpricing': 'type_of_contract_pricing',
            'typeofidc': 'type_of_idc',
            'typeofsetaside': 'type_set_aside',
            'ultimatecompletiondate': 'period_of_perf_potential_e',
            'undefinitized_action_desc': 'undefinitized_action_desc',
            'us_government_entity': 'us_government_entity',
            'useofepadesignatedproducts': 'epa_designated_product',
            'vendor_cd': 'legal_entity_congressional',
            'vendoralternatename': 'vendor_alternate_name',
            'vendoralternatesitecode': 'vendor_alternate_site_code',
            'vendorcountrycode': 'legal_entity_country_code',
            'vendordoingasbusinessname': 'vendor_doing_as_business_n',
            'vendorenabled': 'vendor_enabled',
            'vendorlegalorganizationname': 'vendor_legal_org_name',
            'vendorlocationdisableflag': 'vendor_location_disabled_f',
            'vendorname': 'awardee_or_recipient_legal',
            'vendorsitecode': 'vendor_site_code',
            'veteranownedflag': 'veteran_owned_business',
            'materials_supplies_descrip': 'materials_supplies_descrip',
            'walshhealyact': 'materials_supplies_article',
            'womenownedflag': 'woman_owned_business',
            'zipcode': 'legal_entity_zip4'
        }, {}
    )

    # make a pass through the dataframe, changing any empty values to None, to ensure that those are represented as
    # NULL in the db.
    cdata = cdata.replace(np.nan, '', regex=True)
    cdata = cdata.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

    return cdata


def get_data_after_colon(row, header):
    # return the data after the colon in the row, or None
    if ':' in str(row[header]):
        colon_loc = str(row[header]).find(':') + 1
        return str(row[header])[colon_loc:].strip()
    return None


def get_data_before_colon(row, header):
    # return the data before the colon in the row, or all the data if there is no colon
    if ':' in str(row[header]):
        return str(row[header]).split(':')[0]
    return str(row[header])


def map_legal_entity_state_code(row):
    # only return a value if the country code is USA
    if row['vendorcountrycode'] and (str(row['vendorcountrycode']).upper() == "USA" or
                                     str(row['vendorcountrycode']).upper() == "UNITED STATES"):
        return str(row['vendor_state_code'])
    return None


def map_legal_entity_state_descrip(row):
    # if the country code doesn't exist or isn't USA, use the country code as the state description
    if not row['vendorcountrycode'] or (str(row['vendorcountrycode']).upper() != "USA" and
                                        str(row['vendorcountrycode']).upper() != "UNITED STATES"):
        return str(row['vendor_state_code'])
    return None


def map_type(row, mappings):
    if str(row['contractactiontype']) in mappings:
        return mappings[str(row['contractactiontype'])]
    return None


def map_type_description(row, mappings):
    if str(row['contractactiontype']) in mappings:
        return str(row['contractactiontype']).split(' ')[0]
    return None


def map_type_manual(row, header, mappings):
    content = str(row[header])
    if ':' in content:
        content = content.split(':')[0].strip()

    if content in mappings:
        return content
    return None


def map_description_manual(row, header, mappings):
    content = str(row[header])
    if ':' in content:
        content = content.split(':')[0].strip()

    if content in mappings:
        return mappings[content]
    return content.upper()


def map_agency_code(row, header, sub_tier_list):
    try:
        code = str(row[header])
        sub_tier_agency = sub_tier_list[code]
        use_frec = sub_tier_agency.is_frec
        agency_data = sub_tier_agency.frec if use_frec else sub_tier_agency.cgac
        return agency_data.frec_code if use_frec else agency_data.cgac_code
    except KeyError:
        return '999'


def map_agency_name(row, header, sub_tier_list):
    try:
        code = str(row[header])
        sub_tier_agency = sub_tier_list[code]
        use_frec = sub_tier_agency.is_frec
        agency_data = sub_tier_agency.frec if use_frec else sub_tier_agency.cgac
        return agency_data.agency_name
    except KeyError:
        return None


def map_sub_tier_name(row, header, sub_tier_list):
    try:
        code = str(row[header])
        return sub_tier_list[code].sub_tier_agency_name
    except KeyError:
        return None


def map_naics(row, header, naics_list):
    try:
        code = str(row[header])
        return naics_list[code]
    except KeyError:
        return None


def map_pulled_from(row, award_contract, idv):
    field_contents = str(row['contractactiontype'])
    if field_contents in award_contract:
        return 'award'
    if field_contents in idv:
        return 'IDV'
    return None


def format_date(row, header):
    given_date = str(row[header])
    given_date_split = given_date.split('/')
    if given_date == '01/01/1900' or len(given_date_split) != 3:
        return None

    return given_date_split[2] + '-' + given_date_split[0] + '-' + given_date_split[1] + ' 00:00:00'


def create_unique_key(row):
    key_list = ['agencyid', 'idvagencyid', 'piid', 'modnumber', 'idvpiid', 'transactionnumber']
    unique_string = ""
    for item in key_list:
        if len(unique_string) > 0:
            unique_string += "_"
        if row[item] and str(row[item]) != 'nan':
            unique_string += str(row[item])
        else:
            unique_string += "-none-"
    return unique_string


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
    parser.add_argument('-f', '--files', help='Load historical data from files', action='store_true')
    parser.add_argument('-sf', '--subfolder',
                        help='Used in conjunction with -f to indicate which Subfolder to load files from',
                        nargs="+", type=str)
    parser.add_argument('-da', '--dates', help='Used in conjunction with -l to specify dates to gather updates from.'
                                               'Should have 2 arguments, first and last day, formatted YYYY/mm/dd',
                        nargs=2, type=str)
    args = parser.parse_args()

    award_types_award = ["BPA Call", "Definitive Contract", "Purchase Order", "Delivery Order"]
    award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]

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

    if args.all:
        if (not args.delivery and not args.other) or (args.delivery and args.other):
            logger.error("When using the -a flag, please include either -d or -o "
                         "(but not both) to indicate which feeds to read in")
            raise ValueError("When using the -a flag, please include either -d or -o "
                             "(but not both) to indicate which feeds to read in")
        logger.info("Starting at: %s", str(datetime.datetime.now()))

        if False:
            for award_type in award_types_idv:
                get_data("IDV", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                         country_list)
            for award_type in award_types_award:
                if award_type != "Delivery Order":
                    get_data("award", award_type, now, sess, sub_tier_list, county_by_name, county_by_code,
                             state_code_list, country_list)

        elif args.delivery:
            get_data("award", "Delivery Order", now, sess, sub_tier_list, county_by_name, county_by_code,
                     state_code_list, country_list)

        last_update = sess.query(FPDSUpdate).one_or_none()

        if last_update:
            sess.query(FPDSUpdate).update({"update_date": now}, synchronize_session=False)
        else:
            sess.add(FPDSUpdate(update_date=now))

        logger.info("Ending at: %s", str(datetime.datetime.now()))

        sess.commit()
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
                     country_list, last_update, start_date=start_date, end_date=end_date)

        for award_type in award_types_award:
            get_data("award", award_type, now, sess, sub_tier_list, county_by_name, county_by_code, state_code_list,
                     country_list, last_update, start_date=start_date, end_date=end_date)

        # We also need to process the delete feed
        get_delete_data("IDV", now, sess, last_update, start_date, end_date)
        get_delete_data("award", now, sess, last_update, start_date, end_date)
        if not start_date and not end_date:
            sess.query(FPDSUpdate).update({"update_date": now}, synchronize_session=False)

        logger.info("Ending at: %s", str(datetime.datetime.now()))
        sess.commit()
    elif args.files:
        logger.info("Starting file loads at: %s", str(datetime.datetime.now()))
        max_year = 2015
        subfolder = None
        if args.subfolder:
            if len(args.subfolder) != 1:
                logger.error("When using the -sf flag, please enter just one string for the folder name")
                raise ValueError("When using the -sf flag, please enter just one string for the folder name")
            subfolder = args.subfolder[0]

        if CONFIG_BROKER["use_aws"]:
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            # get naics dictionary
            s3bucket_naics = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
            agency_list_path = s3bucket_naics.get_key("naics.csv").generate_url(expires_in=600)
            agency_list_file = urllib.request.urlopen(agency_list_path)
            reader = csv.reader(agency_list_file.read().decode("utf-8").splitlines())
            naics_dict = {rows[0]: rows[1].upper() for rows in reader}

            # parse contracts files
            s3bucket = s3connection.lookup(CONFIG_BROKER['archive_bucket'])
            if subfolder:
                subfolder = subfolder + "/"
            for key in s3bucket.list(prefix=subfolder):
                match_string = '^\d{4}_All_Contracts_Full_\d{8}.csv.zip'
                if subfolder:
                    match_string = "^" + subfolder + "\d{4}_All_Contracts_Full_\d{8}.csv.zip"
                if re.match(match_string, key.name):
                    # we only want up through 2015 for this data unless it’s a subfolder, then do all of them
                    if subfolder or int(key.name[:4]) <= max_year:
                        # Create an in-memory bytes IO buffer
                        with io.BytesIO() as b:
                            # Read the file into it
                            key.get_file(b)

                            # Reset the file pointer to the beginning
                            parse_fpds_file(b, sess, sub_tier_list, naics_dict, filename=key.name)
        else:
            # get naics dictionary
            naics_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
            with open(os.path.join(naics_path, 'naics.csv'), 'r') as f:
                reader = csv.reader(f)
                naics_dict = {rows[0]: rows[1].upper() for rows in reader}

            # parse contracts files
            base_path = os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config", "fabs")
            if subfolder:
                base_path = os.path.join(base_path, subfolder)
            file_list = [f for f in os.listdir(base_path)]
            for file in file_list:
                if re.match('^\d{4}_All_Contracts_Full_\d{8}.csv.zip', file):
                    # we only want up through 2015 for this data
                    if int(file[:4]) <= max_year:
                        parse_fpds_file(open(os.path.join(base_path, file)).name, sess, sub_tier_list, naics_dict)

        logger.info("Ending at: %s", str(datetime.datetime.now()))
        sess.commit()
    # TODO add a correct start date for "all" so we don't get ALL the data or too little of the data
    # TODO fine-tune indexing

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
