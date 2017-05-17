import argparse
import requests
import xmltodict

import datetime
import re

from sqlalchemy.dialects.postgresql import insert

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import CGAC, SubTierAgency
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.jobModels import FPDSUpdate

from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app

feed_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=PUBLIC&templateName=1.4.5&q="
delete_url = "https://www.fpds.gov/ezsearch/FEEDS/ATOM?FEEDNAME=DELETED&templateName=1.4.5&q="


def list_data(data):
    if isinstance(data, dict):
        # make a list so it's consistent
        data = [data, ]
    return data


def extract_text(data_val):
    if type(data_val) is not str:
        data_val = data_val['#text']
    return data_val


# Get values from the awardID level of the xml
def award_id_values(data, obj):
    try:
        obj['award_modification_amendme'] = extract_text(data['awardContractID']['modNumber'])
    except (KeyError, TypeError):
        obj['award_modification_amendme'] = None
    try:
        obj['transaction_number'] = extract_text(data['awardContractID']['transactionNumber'])
    except (KeyError, TypeError):
        obj['transaction_number'] = None
    try:
        obj['piid'] = extract_text(data['awardContractID']['PIID'])
    except (KeyError, TypeError):
        obj['piid'] = None

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
    value_map = {'modNumber': 'award_modification_amendme',
                 'PIID': 'piid'}
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


# Get values from the competition level of the xml
def competition_values(data, obj):
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


# Get values from the contractData level of the xml
def contract_data_values(data, obj, atom_type):
    value_map = {'consolidatedContract': 'consolidated_contract',
                 'contingencyHumanitarianPeacekeepingOperation': 'contingency_humanitarian_o',
                 'contractFinancing': 'contract_financing',
                 'costAccountingStandardsClause': 'cost_accounting_standards',
                 'costOrPricingData': 'cost_or_pricing_data',
                 'descriptionOfContractRequirement': 'award_description',
                 'GFE-GFP': 'government_furnished_equip',
                 'majorProgramCode': 'major_program',
                 'multiYearContract': 'multi_year_contract',
                 'nationalInterestActionCode': 'national_interest_action',
                 'numberOfActions': 'number_of_actions',
                 'performanceBasedServiceContract': 'performance_based_service',
                 'programAcronym': 'program_acronym',
                 'purchaseCardAsPaymentMethod': 'purchase_card_as_payment_m',
                 'reasonForModification': 'action_type',
                 'referencedIDVType': 'referenced_idv_type',
                 'seaTransportation': 'sea_transportation',
                 'solicitationID': 'solicitation_identifier',
                 'typeOfContractPricing': 'type_of_contract_pricing',
                 'typeOfIDC': 'type_of_idc',
                 'undefinitizedAction': 'undefinitized_action'}

    if atom_type == "award":
        value_map['contractActionType'] = 'contract_award_type'
        value_map['referencedIDVMultipleOrSingle'] = 'referenced_mult_or_single'
    else:
        value_map['contractActionType'] = 'idv_type'
        value_map['multipleOrSingleAwardIDC'] = 'multiple_or_single_award_i'
        value_map['referencedIDVMultipleOrSingle'] = 'referenced_mult_or_single'

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
                 'multiYearContract': 'multi_year_contract_desc',
                 'nationalInterestActionCode': 'national_interest_desc',
                 'performanceBasedServiceContract': 'performance_based_se_desc',
                 'purchaseCardAsPaymentMethod': 'purchase_card_as_paym_desc',
                 'reasonForModification': 'action_type_description',
                 'referencedIDVType': 'referenced_idv_type_desc',
                 'seaTransportation': 'sea_transportation_desc',
                 'typeOfContractPricing': 'type_of_contract_pric_desc',
                 'typeOfIDC': 'type_of_idc_description',
                 'undefinitizedAction': 'undefinitized_action_desc'}

    if atom_type == "award":
        value_map['contractActionType'] = 'contract_award_type_desc'
        value_map['referencedIDVMultipleOrSingle'] = 'referenced_mult_or_si_desc'
    else:
        value_map['contractActionType'] = 'idv_type_description'
        value_map['multipleOrSingleAwardIDC'] = 'multiple_or_single_aw_desc'
        value_map['referencedIDVMultipleOrSingle'] = 'referenced_mult_or_si_desc'

    for key, value in value_map.items():
        try:
            obj[value] = data[key]['@description']
        except (KeyError, TypeError):
            obj[value] = None

    return obj


# Get values from the dollarValues level of the xml
def dollar_values_values(data, obj):
    value_map = {'baseAndAllOptionsValue': 'potential_total_value_awar',
                 'baseAndExercisedOptionsValue': 'current_total_value_award',
                 'obligatedAmount': 'federal_action_obligation'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


# Get values from the legislativeMandates level of the xml
def legislative_mandates_values(data, obj):
    value_map = {'ClingerCohenAct': 'clinger_cohen_act_planning',
                 'DavisBaconAct': 'davis_bacon_act',
                 'interagencyContractingAuthority': 'interagency_contracting_au',
                 'otherStatutoryAuthority': 'other_statutory_authority',
                 'serviceContractAct': 'service_contract_act',
                 'WalshHealyAct': 'walsh_healey_act'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data[key])
        except (KeyError, TypeError):
            obj[value] = None

    # get descriptions for things in the value map
    value_map = {'ClingerCohenAct': 'clinger_cohen_act_pla_desc',
                 'DavisBaconAct': 'davis_bacon_act_descrip',
                 'interagencyContractingAuthority': 'interagency_contract_desc',
                 'serviceContractAct': 'service_contract_act_desc',
                 'WalshHealyAct': 'walsh_healey_act_descrip'}

    for key, value in value_map.items():
        try:
            obj[value] = data[key]['@description']
        except (KeyError, TypeError):
            obj[value] = None

    return obj


# Get values from the placeOfPerformance level of the xml
def place_of_performance_values(data, obj, atom_type):
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
    value_map = {'locationCode': 'place_of_performance_locat',
                 'stateCode': 'place_of_performance_state'}

    if atom_type == "award":
        value_map['countryCode'] = 'place_of_perform_country_c'

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


# Get values from the productOrServiceInformation level of the xml
def product_or_service_information_values(data, obj):
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


# Get values from the purchaserInformation level of the xml
def purchaser_information_values(data, obj):
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


# Get values from the relevantContractDates level of the xml
def relevant_contract_dates_values(data, obj):
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


# Get values from the vendor level of the xml
def vendor_values(data, obj):
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
    value_map = {'vendorDoingAsBusinessName': 'vendor_doing_as_business_n',
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


# Get values from the vendorSiteDetails level of the xml (sub-level of vendor)
def vendor_site_details_values(data, obj):
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
    value_map = {'DUNSNumber': 'awardee_or_recipient_uniqu',
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
                 'ZIPCode': 'legal_entity_zip4'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorLocation'][key])
        except (KeyError, TypeError):
            obj[value] = None

    # differentiating between US and foreign states
    key = 'legal_entity_state_code'
    if obj['legal_entity_country_code'] != 'USA':
        key = 'legal_entity_state_descrip'
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
    value_map = {'isForeignOwnedAndLocated': 'foreign_owned_and_located',
                 'isLimitedLiabilityCorporation': 'limited_liability_corporat',
                 'isShelteredWorkshop': 'the_ability_one_program',
                 'isSubchapterSCorporation': 'subchapter_s_corporation'}

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
                 'isWomenOwned': 'woman_owned_business'}

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
                 'isSubContinentAsianAmericanOwnedBusiness': 'subcontinent_asian_asian_i',
                 'isWomenOwnedSmallBusiness': 'women_owned_small_business'}

    for key, value in value_map.items():
        try:
            obj[value] = extract_text(data['vendorSocioEconomicIndicators']['minorityOwned'][key])
        except (KeyError, TypeError):
            obj[value] = None

    return obj


# calculate values that aren't in any feed but can be calculated
def calculate_remaining_fields(obj, sess):
    if obj['awarding_sub_tier_agency_c']:
        agency_data = sess.query(CGAC).\
            filter(CGAC.cgac_id == SubTierAgency.cgac_id,
                   SubTierAgency.sub_tier_agency_code == obj['awarding_sub_tier_agency_c']).one_or_none()
        if agency_data:
            obj['awarding_agency_code'] = agency_data.cgac_code
            obj['awarding_agency_name'] = agency_data.agency_name
        else:
            obj['awarding_agency_code'] = '999'
            obj['awarding_agency_name'] = None

    if obj['funding_sub_tier_agency_co']:
        agency_data = sess.query(CGAC). \
            filter(CGAC.cgac_id == SubTierAgency.cgac_id,
                   SubTierAgency.sub_tier_agency_code == obj['funding_sub_tier_agency_co']).one_or_none()
        if agency_data:
            obj['funding_agency_code'] = agency_data.cgac_code
            obj['funding_agency_name'] = agency_data.agency_name
        else:
            obj['funding_agency_code'] = '999'
            obj['funding_agency_name'] = None
    return obj


# process the data coming in
def process_data(data, atom_type, sess):
    obj = {}

    if atom_type == "award":
        # make sure key exists before passing it
        try:
            data['awardID']
        except KeyError:
            data['awardID'] = {}
        obj = award_id_values(data['awardID'], obj)
    else:
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

    if atom_type == "award":
        # make sure key exists before passing it
        try:
            data['placeOfPerformance']
        except KeyError:
            data['placeOfPerformance'] = {}
        obj = place_of_performance_values(data['placeOfPerformance'], obj, atom_type)

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

    obj = calculate_remaining_fields(obj, sess)

    try:
        obj['last_modified'] = data['transactionInformation']['lastModifiedDate']
    except (KeyError, TypeError):
        obj['last_modified'] = None

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
    obj = {}

    # get all values that make up unique key except awarding_sub_tier_agency_c because that doesn't show up
    # in delete feed
    # try:
    #     obj['awarding_sub_tier_agency_c'] = extract_text(data['purchaserInformation']['contractingOfficeAgencyID'])
    # except (KeyError, TypeError):
    #     obj['awarding_sub_tier_agency_c'] = None

    if atom_type == "award":
        try:
            obj['piid'] = extract_text(data['awardID']['awardContractID']['PIID'])
        except (KeyError, TypeError):
            obj['piid'] = None

        try:
            obj['award_modification_amendme'] = extract_text(data['awardID']['awardContractID']['modNumber'])
        except (KeyError, TypeError):
            obj['award_modification_amendme'] = None

        try:
            obj['parent_award_id'] = extract_text(data['awardID']['referencedIDVID']['PIID'])
        except (KeyError, TypeError):
            obj['parent_award_id'] = None

        try:
            obj['referenced_idv_modificatio'] = extract_text(data['awardID']['referencedIDVID']['modNumber'])
        except (KeyError, TypeError):
            obj['referenced_idv_modificatio'] = None

        try:
            obj['transaction_number'] = extract_text(data['awardID']['awardContractID']['transactionNumber'])
        except (KeyError, TypeError):
            obj['transaction_number'] = None
    else:
        try:
            obj['piid'] = extract_text(data['contractID']['IDVID']['PIID'])
        except (KeyError, TypeError):
            obj['piid'] = None

        try:
            obj['award_modification_amendme'] = extract_text(data['contractID']['IDVID']['modNumber'])
        except (KeyError, TypeError):
            obj['award_modification_amendme'] = None

        try:
            obj['parent_award_id'] = extract_text(data['contractID']['referencedIDVID']['PIID'])
        except (KeyError, TypeError):
            obj['parent_award_id'] = None

        try:
            obj['referenced_idv_modificatio'] = extract_text(data['contractID']['referencedIDVID']['modNumber'])
        except (KeyError, TypeError):
            obj['referenced_idv_modificatio'] = None

        # not in IDV feed, just set it to None
        obj['transaction_number'] = None

    # not sure if I need this yet, we'll see
    try:
        obj['last_modified'] = data['transactionInformation']['lastModifiedDate']
    except (KeyError, TypeError):
        obj['last_modified'] = None

    return obj


def process_and_add(data, contract_type, sess, last_run=None):
    if not last_run:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], atom_type=contract_type, sess=sess)
            tmp_award = DetachedAwardProcurement(**tmp_obj)
            sess.add(tmp_award)
    else:
        for value in data:
            tmp_obj = process_data(value['content'][contract_type], atom_type=contract_type, sess=sess)
            insert_statement = insert(DetachedAwardProcurement).values(**tmp_obj).on_conflict_do_update(
                constraint='uniq_det_award_proc_key',
                set_=tmp_obj)
            sess.execute(insert_statement)


def get_data(contract_type, award_type, now, sess, last_run=None):
    data = []
    yesterday = now - datetime.timedelta(days=1)
    log_interval = 1000
    if not last_run:
        # we want to log less often for a "get all" run
        log_interval = 10000
        # params = 'SIGNED_DATE:[2015/10/01'+ yesterday.strftime('%Y/%m/%d') + '] '
        params = 'SIGNED_DATE:[2016/10/01' + yesterday.strftime('%Y/%m/%d') + '] '
        # params = 'SIGNED_DATE:[2017/03/01,'+ yesterday.strftime('%Y/%m/%d') + '] '
    else:
        last_run_date = last_run.update_date
        params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '

    # TODO remove this later, this is just for testing
    # params += 'CONTRACTING_AGENCY_ID:1542 '
    # params = 'VENDOR_ADDRESS_COUNTRY_CODE:"GBR"'
    # params = 'PIID:"0046"+REF_IDV_PIID:"W56KGZ15A6000"'

    i = 0
    print('Starting: ' + feed_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '" AWARD_TYPE:"' + award_type + '"')
    while True:
        resp = requests.get(feed_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '" AWARD_TYPE:"' + award_type + '"&start=' + str(i), timeout=60)
        resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None})
        # only list the data if there's data to list
        try:
            listed_data = list_data(resp_data['feed']['entry'])
        except KeyError:
            listed_data = []

        for ld in listed_data:
            data.append(ld)
            i += 1

        # Log which one we're on so we can keep track of how far we are (different numbers for different pulls)
        if i % log_interval == 0:
            print("On line " + str(i))

            # every 100,000 records, add the data to the session and clear the data array so we don't run into
            # a memory error. This can be done inside the 100 tracker because 100,000 is divisible by 100
            # so we don't have to check this if statement every time. Don't process on row 0
            if i % 100000 == 0 and i != 0:
                print("Processing next 100,000 records")
                process_and_add(data, contract_type, sess, last_run)
                data = []

        # if we got less than 10 records, we can stop calling the feed
        if len(listed_data) < 10:
            break

    print(i)

    # insert whatever is left
    process_and_add(data, contract_type, sess, last_run)
    print("processed " + contract_type + ": " + award_type + " data")


def get_delete_data(contract_type, now, sess, last_run):
    data = []
    yesterday = now - datetime.timedelta(days=1)
    last_run_date = last_run.update_date
    params = 'LAST_MOD_DATE:[' + last_run_date.strftime('%Y/%m/%d') + ',' + yesterday.strftime('%Y/%m/%d') + '] '

    i = 0
    print('Starting: ' + delete_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '"')
    while True:
        resp = requests.get(delete_url + params + 'CONTRACT_TYPE:"' + contract_type.upper() + '"&start=' + str(i), timeout=60)
        resp_data = xmltodict.parse(resp.text, process_namespaces=True, namespaces={'http://www.fpdsng.com/FPDS': None, 'http://www.w3.org/2005/Atom': None})
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
            print("On line " + str(i))

        # if we got less than 10 records, we can stop calling the feed
        if len(listed_data) < 10:
            break

    print(i)
    for value in data:
        tmp_obj = process_delete_data(value['content'][contract_type], atom_type=contract_type)
        print(tmp_obj)
        # tmp_award = DetachedAwardProcurement(**tmp_obj)
        # sess.add(tmp_award)


def main():
    sess = GlobalDB.db().session

    now = datetime.datetime.now()

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-a', '--all', help='Clear out the database and get historical data', action='store_true')
    parser.add_argument('-l', '--latest', help='Get by last_mod_date stored in DB', action='store_true')
    args = parser.parse_args()

    award_types_award = ["BPA Call", "Purchase Order", "Delivery Order", "Definitive Contract"]
    award_types_idv = ["GWAC", "IDC", "FSS", "BOA", "BPA"]

    if args.all:
        print("Starting at: " + str(datetime.datetime.now()))
        # clear out table
        sess.query(DetachedAwardProcurement).delete()

        # loop through and check all award types
        for award_type in award_types_award:
            get_data("award", award_type, now, sess)
        for award_type in award_types_idv:
            get_data("IDV", award_type, now, sess)

        last_update = sess.query(FPDSUpdate).one_or_none()

        if last_update:
            sess.query(FPDSUpdate).update({"update_date":now}, synchronize_session=False)
        else:
            sess.add(FPDSUpdate(update_date=now))

        print("Ending at: " + str(datetime.datetime.now()))

        sess.commit()
    elif args.latest:
        print("Starting at: " + str(datetime.datetime.now()))

        last_update = sess.query(FPDSUpdate).one_or_none()

        # update_date can't be null because it's being used as the PK for the table, so it can only exist or
        # there are no rows in the table. If there are no rows, act like it's an "add all"
        if not last_update:
            print("No last_update date present, please run the script with the -a flag to generate an initial dataset")
            raise ValueError(
                "No last_update date present, please run the script with the -a flag to generate an initial dataset")

        # loop through and check all award types
        # for award_type in award_types_award:
        #     get_data("award", award_type, sess, last_update)
        # for award_type in award_types_idv:
        #     get_data("IDV", award_type, sess, last_update)

        # We also need to process the delete feed
        get_delete_data("award", now, sess, last_update)
        # sess.query(FPDSUpdate).update({"update_date":now}, synchronize_session=False)

        print("Ending at: " + str(datetime.datetime.now()))
        sess.commit()
    # TODO add a correct start date for "all" so we don't get ALL the data
    # TODO delete feed when inserting not "all" (sub-step, figure out what we're comparing against so it's easier to delete)
    # TODO fine-tune indexing
    # TODO threading

if __name__ == '__main__':
    with create_app().app_context():
        main()
