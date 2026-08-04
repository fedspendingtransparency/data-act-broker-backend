import argparse
import datetime
import json
import logging
import os
import tempfile

from requests.exceptions import RequestException

import boto3
import numpy as np
import pandas as pd

from sqlalchemy import func, case, types, text, Boolean

from dataactbroker.helpers.script_helper import validate_load_dates
from dataactbroker.helpers.generic_helper import unzip

from dataactcore.broker_logging import configure_logging
from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date, get_utc_now, get_timestamp
from dataactcore.models.domainModels import (
    SubTierAgency,
    CGAC,
    FREC,
    CountryCode,
    States,
    CountyCode,
    SAMRecipient,
)
from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.userModel import User  # noqa
from dataactcore.utils.loader_utils import clean_data
from dataactcore.utils.sam_recipient import request_sam_contracts_api
from dataactcore.utils.business_categories import get_business_categories

from dataactvalidator.health_check import create_app


SAM_CONTRACT_MAPPINGS = {
    "contractId.modificationNumber": "award_modification_amendme",
    "contractId.piid": "piid",
    "contractId.reasonForModification.code": "action_type",
    "contractId.reasonForModification.name": "action_type_description",
    "contractId.referencedIDVModificationNumber": "referenced_idv_modificatio",
    "contractId.referencedIDVSubtier.code": "referenced_idv_agency_iden",
    "contractId.referencedIDVSubtier.name": "referenced_idv_agency_desc",
    "contractId.referencedIDVPiid": "parent_award_id",
    "contractId.subtier.code": "agency_id",
    "contractId.transactionNumber": "transaction_number",
    "coreData.acquisitionData.consolidatedContract.code": "consolidated_contract",
    "coreData.acquisitionData.consolidatedContract.name": "consolidated_contract_desc",
    "coreData.acquisitionData.contractFinancing.code": "contract_financing",
    "coreData.acquisitionData.contractFinancing.name": "contract_financing_descrip",
    "coreData.acquisitionData.majorProgramCode": "major_program",
    "coreData.acquisitionData.multipleOrSingleAwardIdc.code": "multiple_or_single_award_i",
    # data is blank for this column for some reason
    "coreData.acquisitionData.multipleOrSingleAwardIdc.name": "multiple_or_single_aw_desc",
    # MISSING, multiYearContract, has data and isn't split but shouldn't have data
    # even when it should have data it isn't split and only provides the desc
    "coreData.acquisitionData.multiyearContract.code": "multi_year_contract",
    # multiYearContract, has data and isn't split but shouldn't have data
    # even when it should have data it isn't split and only provides the desc
    "coreData.acquisitionData.multiyearContract.name": "multi_year_contract_desc",
    "coreData.acquisitionData.nationalInterestAction.code": "national_interest_action",
    "coreData.acquisitionData.nationalInterestAction.name": "national_interest_desc",
    "coreData.acquisitionData.performanceBasedServiceContract.code": "performance_based_service",
    "coreData.acquisitionData.performanceBasedServiceContract.name": "performance_based_se_desc",
    "coreData.acquisitionData.programAcronym": "program_acronym",
    "coreData.acquisitionData.typeOfContractPricing.code": "type_of_contract_pricing",
    "coreData.acquisitionData.typeOfContractPricing.name": "type_of_contract_pric_desc",
    "coreData.acquisitionData.typeOfIdc.code": "type_of_idc",
    "coreData.acquisitionData.typeOfIdc.name": "type_of_idc_description",
    "coreData.awardOrIDV": "pulled_from",
    "coreData.competitionInformation.a76Action.code": "a_76_fair_act_action",
    "coreData.competitionInformation.a76Action.name": "a_76_fair_act_action_desc",
    "coreData.competitionInformation.extentCompeted.code": "extent_competed",
    "coreData.competitionInformation.extentCompeted.name": "extent_compete_description",
    # 0 entries in our DB with data so no way to test, going to have to go on faith
    "coreData.competitionInformation.IDVnumberOfOffersReceived": "idv_number_of_offers_recie",
    "coreData.competitionInformation.localAreaSetAside.code": "local_area_set_aside",
    "coreData.competitionInformation.localAreaSetAside.name": "local_area_set_aside_desc",
    "coreData.competitionInformation.otherThanFullAndOpenCompetition.code": "other_than_full_and_open_c",
    "coreData.competitionInformation.otherThanFullAndOpenCompetition.name": "other_than_full_and_o_desc",
    "coreData.competitionInformation.sbirSTTR.code": "research",
    "coreData.competitionInformation.sbirSTTR.name": "research_description",
    # has data in SAM but doesn't have data in FPDS at least sometimes
    "coreData.competitionInformation.smallBusinessCompetitivenessDemonstrationProgram.name": (
        "small_business_competitive"
    ),
    "coreData.competitionInformation.solicitationProcedures.code": "solicitation_procedures",
    "coreData.competitionInformation.solicitationProcedures.name": "solicitation_procedur_desc",
    "coreData.competitionInformation.sourceSelectionProcess.code": "source_selection_process",
    "coreData.competitionInformation.statutoryExceptionToFairOpportunity.code": "fair_opportunity_limited_s",
    "coreData.competitionInformation.statutoryExceptionToFairOpportunity.name": "fair_opportunity_limi_desc",
    "coreData.competitionInformation.typeOfSetAside.code": "type_set_aside",
    "coreData.competitionInformation.typeOfSetAside.name": "type_set_aside_description",
    "coreData.federalOrganization.contractingInformation.contractingOffice.code": "awarding_office_code",
    "coreData.federalOrganization.contractingInformation.contractingOffice.name": "awarding_office_name",
    "coreData.federalOrganization.contractingInformation.contractingSubtier.code": "awarding_sub_tier_agency_c",
    "coreData.federalOrganization.contractingInformation.contractingSubtier.name": "awarding_sub_tier_agency_n",
    "coreData.federalOrganization.fundingInformation.foreignFunding.code": "foreign_funding",
    "coreData.federalOrganization.fundingInformation.foreignFunding.name": "foreign_funding_desc",
    "coreData.federalOrganization.fundingInformation.fundingOffice.code": "funding_office_code",
    "coreData.federalOrganization.fundingInformation.fundingOffice.name": "funding_office_name",
    "coreData.federalOrganization.fundingInformation.fundingSubtier.code": "funding_sub_tier_agency_co",
    "coreData.federalOrganization.fundingInformation.fundingSubtier.name": "funding_sub_tier_agency_na",
    "coreData.legislativeMandates.clingerCohenAct.code": "clinger_cohen_act_planning",
    "coreData.legislativeMandates.clingerCohenAct.name": "clinger_cohen_act_pla_desc",
    "coreData.legislativeMandates.constructionWageRateRequirements.code": "construction_wage_rate_req",
    "coreData.legislativeMandates.constructionWageRateRequirements.name": "construction_wage_rat_desc",
    "coreData.legislativeMandates.interagencyContractingAuthority.code": "interagency_contracting_au",
    "coreData.legislativeMandates.interagencyContractingAuthority.name": "interagency_contract_desc",
    "coreData.legislativeMandates.laborStandards.code": "labor_standards",
    "coreData.legislativeMandates.laborStandards.name": "labor_standards_descrip",
    "coreData.legislativeMandates.materialsSuppliesArticlesEquipment.code": "materials_supplies_article",
    "coreData.legislativeMandates.materialsSuppliesArticlesEquipment.name": "materials_supplies_descrip",
    "coreData.legislativeMandates.otherStatutoryAuthority": "other_statutory_authority",
    "coreData.preferenceProgramsInformation.priceEvaluationPercentDifference": "price_evaluation_adjustmen",
    "coreData.principalPlaceOfPerformance.city.name": "place_of_perform_city_name",
    "coreData.principalPlaceOfPerformance.congressionalDistrict": "place_of_performance_congr",
    "coreData.principalPlaceOfPerformance.county.code": "place_of_perform_county_co",
    "coreData.principalPlaceOfPerformance.county.name": "place_of_perform_county_na",
    "coreData.principalPlaceOfPerformance.country.code": "place_of_perform_country_c",
    "coreData.principalPlaceOfPerformance.country.name": "place_of_perf_country_desc",
    "coreData.principalPlaceOfPerformance.state.code": "place_of_performance_state",
    "coreData.principalPlaceOfPerformance.state.name": "place_of_perfor_state_desc",
    "coreData.principalPlaceOfPerformance.zipCode": "place_of_performance_zip4a",
    "coreData.productOrServiceInformation.contractBundling.code": "contract_bundling",
    "coreData.productOrServiceInformation.contractBundling.name": "contract_bundling_descrip",
    "coreData.productOrServiceInformation.countryOfOrigin.code": "country_of_product_or_serv",
    "coreData.productOrServiceInformation.countryOfOrigin.name": "country_of_product_or_desc",
    "coreData.productOrServiceInformation.dodAcquisitionProgram.code": "program_system_or_equipmen",
    "coreData.productOrServiceInformation.dodAcquisitionProgram.name": "program_system_or_equ_desc",
    "coreData.productOrServiceInformation.dodClaimantProgram.code": "dod_claimant_program_code",
    "coreData.productOrServiceInformation.dodClaimantProgram.name": "dod_claimant_prog_cod_desc",
    "coreData.productOrServiceInformation.gfeGfp.code": "government_furnished_prope",
    "coreData.productOrServiceInformation.gfeGfp.name": "government_furnished_desc",
    "coreData.productOrServiceInformation.informationTechnologyCommercialItemCategory.code": (
        "information_technology_com"
    ),
    "coreData.productOrServiceInformation.informationTechnologyCommercialItemCategory.name": (
        "information_technolog_desc"
    ),
    "coreData.productOrServiceInformation.principalNaics[0].code": "naics",
    "coreData.productOrServiceInformation.principalNaics[0].name": "naics_description",
    "coreData.productOrServiceInformation.productOrService.code": "product_or_service_code",
    "coreData.productOrServiceInformation.productOrService.name": "product_or_service_co_desc",
    "coreData.productOrServiceInformation.recoveredMaterialClauses.code": "recovered_materials_sustai",
    "coreData.productOrServiceInformation.recoveredMaterialClauses.name": "recovered_materials_s_desc",
    "coreData.solicitationDate": "solicitation_date",
    "coreData.solicitationId": "solicitation_identifier",
    "awardDetails.awardeeData.awardeeAlternateSiteCode": "vendor_alternate_site_code",
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.corporateEntityNotTaxExempt": (
        "corporate_entity_not_tax_e"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.corporateEntityTaxExempt": (
        "corporate_entity_tax_exemp"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.internationalOrganization": (
        "international_organization"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.partnershipOrLimitedLiabilityPartnership": (
        "partnership_or_limited_lia"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.smallAgriculturalCooperative": (
        "small_agricultural_coopera"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.businessOrOrganization.soleProprietorship": "sole_proprietorship",
    "awardDetails.awardeeData.awardeeBusinessTypes.communityDevelopmentCorporationOwnedConcern": (
        "community_developed_corpor"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.foreignGovernment": "foreign_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.city": "city_local_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.county": "county_local_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.intermunicipal": "inter_municipal_local_gove",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.localGovernmentOwned": "local_government_owned",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.municipality": "municipality_local_governm",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.schooldistrict": "school_district_local_gove",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.township": "township_local_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsLocalGovernment.usLocalGovernment": "us_local_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsFederalGovernment.federalAgency": "federal_agency",
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsFederalGovernment.federallyFundedResearchAndDevelopmentCorp": (
        "federally_funded_research"
    ),
    "awardDetails.awardeeData.awardeeBusinessTypes.isUsFederalGovernment.usFederalGovernment": "us_federal_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.laborSurplusAreaFirm": "labor_surplus_area_firm",
    "awardDetails.awardeeData.awardeeBusinessTypes.usGovernmentEntity": "us_government_entity",
    "awardDetails.awardeeData.awardeeBusinessTypes.usStateGovernment": "us_state_government",
    "awardDetails.awardeeData.awardeeBusinessTypes.usTribalGovernment": "us_tribal_government",
    "awardDetails.awardeeData.awardeeHeader.awardeeAlternateName": "vendor_alternate_name",
    "awardDetails.awardeeData.awardeeHeader.awardeeDoingBusinessAsName": "vendor_doing_as_business_n",
    # 0 entries in our DB with data so no way to test, going to have to go on faith
    "awardDetails.awardeeData.awardeeHeader.awardeeEnabled": "vendor_enabled",
    "awardDetails.awardeeData.awardeeHeader.awardeeName": "awardee_or_recipient_legal",
    "awardDetails.awardeeData.awardeeHeader.legalBusinessName": "uei_legal_business_name",
    "awardDetails.awardeeData.awardeeLocation.awardeeDataSource": "entity_data_source",
    # 0 entries in our DB with data so no way to test, going to have to go on faith
    "awardDetails.awardeeData.awardeeLocation.awardeeLocationDisabledFlag": "vendor_location_disabled_f",
    "awardDetails.awardeeData.awardeeLocation.city": "legal_entity_city_name",
    "awardDetails.awardeeData.awardeeLocation.congressionalDistrict": "legal_entity_congressional",
    "awardDetails.awardeeData.awardeeLocation.country.code": "legal_entity_country_code",
    "awardDetails.awardeeData.awardeeLocation.country.name": "legal_entity_country_name",
    "awardDetails.awardeeData.awardeeLocation.faxNumber": "vendor_fax_number",
    "awardDetails.awardeeData.awardeeLocation.phoneNumber": "vendor_phone_number",
    # NULL if foreign
    "awardDetails.awardeeData.awardeeLocation.state.code": "legal_entity_state_code",
    # use awardDetails.awardeeData.awardeeLocation.state.code if foreign
    "awardDetails.awardeeData.awardeeLocation.state.name": "legal_entity_state_descrip",
    "awardDetails.awardeeData.awardeeLocation.streetAddress1": "legal_entity_address_line1",
    "awardDetails.awardeeData.awardeeLocation.streetAddress2": "legal_entity_address_line2",
    "awardDetails.awardeeData.awardeeLocation.streetAddress3": "legal_entity_address_line3",
    "awardDetails.awardeeData.awardeeLocation.zip": "legal_entity_zip4",
    "awardDetails.awardeeData.awardeeRegistrationDetails.divisionName": "division_name",
    "awardDetails.awardeeData.awardeeRegistrationDetails.divisionNumberOrOfficeCode": "division_number_or_office",
    # No data in our DB, going to have to go on faith
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeDomesticParentUEI": "domestic_parent_uei",
    # No data in our DB, going to have to go on faith
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeDomesticParentName": "domestic_parent_uei_name",
    # 0 entries in our DB with data so no way to test, going to have to go on faith
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeImmediateParentName": "immediate_parent_uei_name",
    # 0 entries in our DB with data so no way to test, going to have to go on faith
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeImmediateParentUEI": "immediate_parent_uei",
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeUltimateParentName": "ultimate_parent_legal_enti",
    "awardDetails.awardeeData.awardeeUEIInformation.awardeeUltimateParentUniqueEntityId": "ultimate_parent_uei",
    "awardDetails.awardeeData.awardeeUEIInformation.cageCode": "cage_code",
    "awardDetails.awardeeData.awardeeUEIInformation.uniqueEntityId": "awardee_or_recipient_uei",
    "awardDetails.awardeeData.certifications.dotCertifiedDisadvantagedBusinessEnterprise": "dot_certified_disadvantage",
    "awardDetails.awardeeData.certifications.sbaCertified8aJointVenture": "sba_certified_8_a_joint_ve",
    "awardDetails.awardeeData.certifications.sbaCertified8aProgramParticipant": "c8a_program_participant",
    "awardDetails.awardeeData.certifications.sbaCertifiedEconomicallyDisadvantagedWomenOwnedSmallBusiness": (
        "sba_cert_econ_disadv_wosb"
    ),
    "awardDetails.awardeeData.certifications.sbaCertifiedHubZoneFirm": "historically_underutilized",
    "awardDetails.awardeeData.certifications.sbaCertifiedSmallDisadvantagedBusiness": "small_disadvantaged_busine",
    "awardDetails.awardeeData.certifications.sbaCertifiedWomenOwnedSmallBusiness": "sba_cert_women_own_small_bus",
    "awardDetails.awardeeData.certifications.selfCertifiedHubZoneJointVenture": "self_cert_hub_zone_joint",
    "awardDetails.awardeeData.certifications.selfCertifiedSmallDisadvantagedBusiness": "self_certified_small_disad",
    "awardDetails.awardeeData.educationalEntities.1862LandGrantCollege": "c1862_land_grant_college",
    "awardDetails.awardeeData.educationalEntities.1890LandGrantCollege": "c1890_land_grant_college",
    "awardDetails.awardeeData.educationalEntities.1994LandGrantCollege": "c1994_land_grant_college",
    "awardDetails.awardeeData.educationalEntities.alaskanNativeServicingInstitution": "alaskan_native_servicing_i",
    "awardDetails.awardeeData.educationalEntities.historicallyBlackCollegeOrUniversity": "historically_black_college",
    "awardDetails.awardeeData.educationalEntities.minorityInstitution": "minority_institution",
    "awardDetails.awardeeData.educationalEntities.nativeHawaiianServicingInstitution": "native_hawaiian_servicing",
    "awardDetails.awardeeData.educationalEntities.privateUniversityOrCollege": "private_university_or_coll",
    "awardDetails.awardeeData.educationalEntities.schoolOfForestry": "school_of_forestry",
    "awardDetails.awardeeData.educationalEntities.stateControlledInstitutionOfHigherLearning": (
        "state_controlled_instituti"
    ),
    "awardDetails.awardeeData.educationalEntities.tribalCollege": "tribal_college",
    "awardDetails.awardeeData.educationalEntities.veterinaryCollege": "veterinary_college",
    "awardDetails.awardeeData.far41102Exception.code": "sam_exception",
    "awardDetails.awardeeData.far41102Exception.name": "sam_exception_description",
    "awardDetails.awardeeData.lineOfBusiness.communityDevelopmentCorporation": "community_development_corp",
    "awardDetails.awardeeData.lineOfBusiness.domesticShelter": "domestic_shelter",
    "awardDetails.awardeeData.lineOfBusiness.educationalInstitution": "educational_institution",
    "awardDetails.awardeeData.lineOfBusiness.foundation": "foundation",
    "awardDetails.awardeeData.lineOfBusiness.hispanicServicingInstitution": "hispanic_servicing_institu",
    "awardDetails.awardeeData.lineOfBusiness.hospital": "hospital_flag",
    "awardDetails.awardeeData.lineOfBusiness.manufacturerOfGoods": "manufacturer_of_goods",
    "awardDetails.awardeeData.lineOfBusiness.veterinaryHospital": "veterinary_hospital",
    "awardDetails.awardeeData.organizationFactors.foreignOwned": "foreign_owned_and_located",
    "awardDetails.awardeeData.organizationFactors.limitedLiabilityCorporation": "limited_liability_corporat",
    "awardDetails.awardeeData.organizationFactors.organizationType": "organizational_type",
    "awardDetails.awardeeData.organizationFactors.profitStructure.forProfitOrganization": "for_profit_organization",
    "awardDetails.awardeeData.organizationFactors.profitStructure.nonProfitOrganization": "nonprofit_organization",
    "awardDetails.awardeeData.organizationFactors.profitStructure.otherNotForProfitOrganization": (
        "other_not_for_profit_organ"
    ),
    "awardDetails.awardeeData.organizationFactors.subchapterSCorporation": "subchapter_s_corporation",
    "awardDetails.awardeeData.organizationFactors.theAbilityOneProgram": "the_ability_one_program",
    "awardDetails.awardeeData.otherGovernmentalEntities.airportAuthority": "airport_authority",
    "awardDetails.awardeeData.otherGovernmentalEntities.councilOfGovernments": "council_of_governments",
    "awardDetails.awardeeData.otherGovernmentalEntities.housingAuthoritiesPublicTribal": "housing_authorities_public",
    "awardDetails.awardeeData.otherGovernmentalEntities.interstateEntity": "interstate_entity",
    "awardDetails.awardeeData.otherGovernmentalEntities.planningCommission": "planning_commission",
    "awardDetails.awardeeData.otherGovernmentalEntities.portAuthority": "port_authority",
    "awardDetails.awardeeData.otherGovernmentalEntities.transitAuthority": "transit_authority",
    "awardDetails.awardeeData.relationshipWithFederalGovernment.allawards": "receives_contracts_and_gra",
    "awardDetails.awardeeData.relationshipWithFederalGovernment.contracts": "contracts",
    "awardDetails.awardeeData.relationshipWithFederalGovernment.federalassistanceawards": "grants",
    "awardDetails.awardeeData.socioEconomicData.alaskanNativeCorporationOwnedFirm": "alaskan_native_owned_corpo",
    "awardDetails.awardeeData.socioEconomicData.americanIndianOwned": "american_indian_owned_busi",
    "awardDetails.awardeeData.socioEconomicData.economicallyDisadvantagedWomenOwnedSmallBusiness": (
        "economically_disadvantaged"
    ),
    "awardDetails.awardeeData.socioEconomicData.economicallyDisadvantagedWomenOwnedSmallBusinessJointVenture": (
        "joint_venture_economically"
    ),
    "awardDetails.awardeeData.socioEconomicData.emergingSmallBusiness": "emerging_small_business",
    "awardDetails.awardeeData.socioEconomicData.indianTribeFederallyRecognized": "indian_tribe_federally_rec",
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.asianPacificAmericanOwned": (
        "asian_pacific_american_own"
    ),
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.blackAmericanOwned": (
        "black_american_owned_busin"
    ),
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.hispanicAmericanOwned": (
        "hispanic_american_owned_bu"
    ),
    # splitting this over two lines, too long for black and flake8
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness."
    "individualOrConcernOtherThanOneOfThePreceding": "other_minority_owned_busin",
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.minorityOwnedBusiness": (
        "minority_owned_business"
    ),
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.nativeAmericanOwned": (
        "native_american_owned_busi"
    ),
    "awardDetails.awardeeData.socioEconomicData.isMinorityOwnedBusiness.subcontinentAsianAsianIndianAmericanOwned": (
        "subcontinent_asian_asian_i"
    ),
    "awardDetails.awardeeData.socioEconomicData.nativeHawaiianOrganizationOwnedFirm": "native_hawaiian_owned_busi",
    "awardDetails.awardeeData.socioEconomicData.serviceDisabledVeteranOwnedBusiness": "service_disabled_veteran_o",
    "awardDetails.awardeeData.socioEconomicData.serviceDisabledVeteranOwnedBusinessJointVenture": (
        "ser_disabvet_own_bus_join_ven"
    ),
    "awardDetails.awardeeData.socioEconomicData.smallBusinessJointVenture": "small_business_joint_venture",
    "awardDetails.awardeeData.socioEconomicData.triballyOwnedFirm": "tribally_owned_business",
    "awardDetails.awardeeData.socioEconomicData.veteranOwnedBusiness": "veteran_owned_business",
    "awardDetails.awardeeData.socioEconomicData.womenOwnedBusiness": "woman_owned_business",
    "awardDetails.awardeeData.socioEconomicData.womenOwnedSmallBusiness": "women_owned_small_business",
    "awardDetails.awardeeData.socioEconomicData.womenOwnedSmallBusinessJointVenture": "joint_venture_women_owned",
    "awardDetails.competitionInformation.commercialItemTestProgram.code": "commercial_item_test_progr",
    "awardDetails.competitionInformation.commercialItemTestProgram.name": "commercial_item_test_desc",
    "awardDetails.competitionInformation.commercialProductsAndServicesAcquisitionProcedures.code": (
        "commercial_item_acquisitio"
    ),
    "awardDetails.competitionInformation.commercialProductsAndServicesAcquisitionProcedures.name": (
        "commercial_item_acqui_desc"
    ),
    "awardDetails.competitionInformation.contractOpportunitiesNotice.code": "fed_biz_opps",
    "awardDetails.competitionInformation.contractOpportunitiesNotice.name": "fed_biz_opps_description",
    "awardDetails.competitionInformation.evaluatedPreference.code": "evaluated_preference",
    "awardDetails.competitionInformation.evaluatedPreference.name": "evaluated_preference_desc",
    "awardDetails.competitionInformation.IDVTypeOfSetAside.code": "idv_type_of_set_aside",
    "awardDetails.competitionInformation.numberOfOffersReceived": "number_of_offers_received",
    "awardDetails.contractData.costAccountingStandardsClause.code": "cost_accounting_standards",
    "awardDetails.contractData.costAccountingStandardsClause.name": "cost_accounting_stand_desc",
    "awardDetails.contractData.costOrPricingData.code": "cost_or_pricing_data",
    "awardDetails.contractData.costOrPricingData.name": "cost_or_pricing_data_desc",
    "awardDetails.contractData.emergencyAcquisition.code": "contingency_humanitarian_o",
    "awardDetails.contractData.emergencyAcquisition.name": "contingency_humanitar_desc",
    "awardDetails.contractData.natureOfServices.code": "inherently_government_func",
    "awardDetails.contractData.natureOfServices.name": "inherently_government_desc",
    # has data in SAM but doesn't have data in FPDS at least sometimes
    "awardDetails.contractData.numberOfActions": "number_of_actions",
    # MISSING EVEN WHEN DATA SHOULD EXIST (ex: PIID: HC102818F1281) (purchaseCardAsPaymentMethod)
    "awardDetails.contractData.purchaseCardAsPaymentMethod.code": "purchase_card_as_payment_m",
    # have already found bad data in it that shouldn't exist
    "awardDetails.contractData.purchaseCardAsPaymentMethod.name": "purchase_card_as_paym_desc",
    "awardDetails.contractData.referencedIDVMultipleOrSingle.code": "referenced_mult_or_single",
    # have already found bad data in it that shouldn't exist
    "awardDetails.contractData.referencedIDVMultipleOrSingle.name": "referenced_mult_or_si_desc",
    "awardDetails.contractData.referencedIDVType.code": "referenced_idv_type",
    "awardDetails.contractData.referencedIDVType.name": "referenced_idv_type_desc",
    "awardDetails.contractData.undefinitizedAction.code": "undefinitized_action",
    "awardDetails.contractData.undefinitizedAction.name": "undefinitized_action_desc",
    "awardDetails.dates.currentCompletionDate": "period_of_performance_curr",
    "awardDetails.dates.dateSigned": "action_date",
    "awardDetails.dates.lastDateToOrder": "ordering_period_end_date",
    "awardDetails.dates.periodOfPerformanceStartDate": "period_of_performance_star",
    "awardDetails.dates.ultimateCompletionDate": "period_of_perf_potential_e",
    "awardDetails.dollars.actionObligation": "federal_action_obligation",
    "awardDetails.dollars.baseAndAllOptionsValue": "base_and_all_options_value",
    "awardDetails.dollars.baseAndExercisedOptionsValue": "base_exercised_options_val",
    "awardDetails.dollars.feePaidForUseOfService": "fee_paid_for_use_of_serv",
    "awardDetails.dollars.totalEstimatedOrderValue": "total_estimated_order_val",
    "awardDetails.legislativeMandates.additionalReporting.code": "additional_reporting_code",
    "awardDetails.legislativeMandates.additionalReporting.name": "additional_reporting_name",
    "awardDetails.preferenceProgramsInformation.contractingOfficerBusinessSizeDetermination[0].code": (
        "contracting_officers_deter"
    ),
    "awardDetails.preferenceProgramsInformation.contractingOfficerBusinessSizeDetermination[0].name": (
        "contracting_officers_desc"
    ),
    "awardDetails.preferenceProgramsInformation.subcontractPlan.code": "subcontracting_plan",
    "awardDetails.preferenceProgramsInformation.subcontractPlan.name": "subcontracting_plan_desc",
    "awardDetails.productOrServiceInformation.descriptionOfContractRequirement": "award_description",
    "awardDetails.productOrServiceInformation.domesticOrForeignEntity.code": "domestic_or_foreign_entity",
    "awardDetails.productOrServiceInformation.domesticOrForeignEntity.name": "domestic_or_foreign_e_desc",
    "awardDetails.productOrServiceInformation.placeOfManufacture.code": "place_of_manufacture",
    "awardDetails.productOrServiceInformation.placeOfManufacture.name": "place_of_manufacture_desc",
    "awardDetails.productOrServiceInformation.seaTransportation.code": "sea_transportation",
    "awardDetails.productOrServiceInformation.seaTransportation.name": "sea_transportation_desc",
    "awardDetails.productOrServiceInformation.useOfEpaDesignatedProducts.code": "epa_designated_product",
    "awardDetails.productOrServiceInformation.useOfEpaDesignatedProducts.name": "epa_designated_produc_desc",
    "awardDetails.totalContractDollars.totalActionObligation": "total_obligated_amount",
    "awardDetails.totalContractDollars.totalBaseAndAllOptionsValue": "potential_total_value_awar",
    "awardDetails.totalContractDollars.totalBaseAndExercisedOptionsValue": "current_total_value_award",
    "awardDetails.transactionData.approvedDate": "approved_date",
    "awardDetails.transactionData.closedDate": "closed_date",
    "awardDetails.transactionData.createdDate": "initial_report_date",
    "awardDetails.transactionData.lastModifiedDate": "last_modified",
}
AWARD_MAPPINGS = {
    "coreData.awardOrIDVType.code": "contract_award_type",
    "coreData.awardOrIDVType.name": "contract_award_type_desc",
}

IDV_MAPPINGS = {
    "coreData.awardOrIDVType.code": "idv_type",
    "coreData.awardOrIDVType.name": "idv_type_description",
}

boolean_fields = [col.name for col in DetachedAwardProcurement.__table__.columns if isinstance(col.type, Boolean)]

date_fields = [
    "action_date",
    "approved_date",
    "initial_report_date",
    "last_modified",
    "period_of_perf_potential_e",
    "period_of_performance_curr",
    "period_of_performance_star",
    "solicitation_date",
]

S3_ARCHIVE = CONFIG_BROKER["sam"]["recipient"]["csv_archive_bucket"]

# Used to determine if it's possible we didn't get all the records in this pull and chunk it
CHUNK_SIZE = 5000
SAM_MAX_FILE_LENGTH = 1000000

# Used for tracking cgac errors for output later
cgac_errors = {}

feed_url = "https://api.sam.gov/contract-awards/v1/search?"
logger = logging.getLogger(__name__)


def insert_into_db(sess, contract_df):
    """Insert the dataframe into the database

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to insert into the database
    """
    # Header column list, remove all quotes and spaces created because we don't need them
    header_cols = str(list(contract_df.columns))[1:-1].replace("'", "").replace(" ", "")

    # Create list of all columns to update, we don't want to update created_at when upserting
    update_list = []
    for col in header_cols.split(","):
        if col != "created_at":
            update_list.append(f"{col} = EXCLUDED.{col}")

    # Replace all nans with nulls
    contract_df = contract_df.replace({np.NaN: "NULL"})

    # Update list columns to be strings for the insert
    contract_df["business_categories"] = contract_df.apply(
        lambda row: "{" + ",".join(row["business_categories"]) + "}", axis=1
    )

    # Remove the T and Z from date columns
    contract_df[date_fields] = contract_df[date_fields].replace({"T": " ", "Z": ""}, regex=True)

    # Execute SQL
    params_dict = {}
    values_list = []
    index = 1
    # take each row from the dataframe
    for row in list(contract_df.to_records(index=False)):
        row_values = []
        # split it up by value for sql injection checking
        for value in row:
            params_dict[f"param{index}"] = str(value).replace('"', "'") if value != "NULL" else None
            row_values.append(f":param{index}")
            index += 1
        # build the row of *params* that will be populated by the binding params_dict
        # and then each *row* needs commas in between them
        values_list.append(f"({','.join(row_values)})")

    sess.execute(
        text(
            f"""
            INSERT INTO detached_award_procurement ({header_cols})
            VALUES {','.join(values_list)}
            ON CONFLICT (detached_award_proc_unique) DO UPDATE SET {",\n".join(update_list)};
        """
        ),
        params_dict,
    )


def calculate_ppop_fields(sess, contract_df, county_df, state_df, country_df, us_territories_df):
    """Calculate values that aren't in any feed (or haven't been provided properly) for place of performance

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to update with derivations
        county_df: a dataframe containing all county codes and names by state
        state_df: a dataframe containing all state codes and names
        country_df: a dataframe containing all country codes and names
        us_territories_df: a dataframe containing all the us and territory 2-char country codes, keyed by country code

    Returns:
        The contract_df dataframe with legal entity derivations
    """
    # Change US territories to the USA country code and name
    us_territories = list(us_territories_df["country_code"])
    ppop_us_mask = contract_df["place_of_perform_country_c"].isin(us_territories)
    ppop_territory_mask = ppop_us_mask & (contract_df["place_of_perform_country_c"] != "USA")

    contract_df = contract_df.merge(
        us_territories_df, how="left", left_on="place_of_perform_country_c", right_on="country_code"
    ).drop("country_code", axis=1)
    contract_df["place_of_performance_state"] = np.where(
        ppop_territory_mask,
        contract_df["country_code_2_char"],
        contract_df["place_of_performance_state"],
    )

    contract_df = contract_df.merge(
        state_df, how="left", left_on="place_of_performance_state", right_on="state_code"
    ).drop("state_code", axis=1)
    # updating both blank states and those that were changed due to the territory updates
    contract_df["place_of_perfor_state_desc"] = np.where(
        ppop_territory_mask | (contract_df["place_of_perfor_state_desc"].isnull()),
        contract_df["state_name"],
        contract_df["place_of_perfor_state_desc"],
    )

    contract_df["place_of_perf_country_desc"] = np.where(
        ppop_territory_mask, "UNITED STATES", contract_df["place_of_perf_country_desc"]
    )
    contract_df["place_of_perform_country_c"] = np.where(
        ppop_territory_mask, "USA", contract_df["place_of_perform_country_c"]
    )

    # Derive ppop country name
    contract_df = contract_df.merge(
        country_df, how="left", left_on="place_of_perform_country_c", right_on="country_code"
    )
    contract_df["place_of_perf_country_desc"] = np.where(
        contract_df["place_of_perf_country_desc"].isnull(),
        contract_df["country_name"],
        contract_df["place_of_perf_country_desc"],
    )

    ppop_valid_zip_mask = (contract_df["place_of_performance_zip4a"].str.match(r"^\d{5}(-?\d{4})?$")) & ppop_us_mask
    # if we have content in the zip code and it's in a valid US format, split it into 5 and 4 digit
    contract_df["place_of_performance_zip5"] = np.where(
        ppop_valid_zip_mask, contract_df["place_of_performance_zip4a"].str[:5], np.nan
    )
    contract_df["place_of_perform_zip_last4"] = np.where(
        ppop_valid_zip_mask & (contract_df["place_of_performance_zip4a"].str.len() > 5),
        contract_df["place_of_performance_zip4a"].str[-4:],
        np.nan,
    )

    # Derive ppop county data
    # Use zip codes to derive remaining possible county codes
    ppop_zips_df = contract_df[["place_of_performance_zip5", "place_of_perform_zip_last4"]][
        (~contract_df["place_of_performance_zip5"].isnull()) & (contract_df["place_of_perform_county_co"].isnull())
    ].drop_duplicates()
    ppop_zips_df["county_code"] = np.nan
    ppop_zips_df.to_sql(
        "tmp_zips_df",
        con=sess.connection(),
        if_exists="replace",
        index=False,
        dtype={
            "place_of_performance_zip5": types.TEXT(),
            "place_of_perform_zip_last4": types.TEXT(),
            "county_code": types.TEXT(),
        },
    )
    # Get 9-digit-related county code
    sess.execute(
        """
                UPDATE tmp_zips_df
                SET county_code = county_number
                FROM zips
                WHERE place_of_performance_zip5 = zip5
                    AND place_of_perform_zip_last4 = zip_last4;
            """
    )

    # Get 5-digit-related county code
    sess.execute(
        """
                UPDATE tmp_zips_df
                SET county_code = county_number
                FROM (
                    SELECT zip5,
                        county_number,
                        ROW_NUMBER() OVER (PARTITION BY
                            zip5
                            ORDER BY county_number
                        ) AS row
                    FROM zips_grouped
                    ) duplicates
                WHERE duplicates.row = 1
                    AND place_of_performance_zip5 = zip5
                    AND county_code IS NULL;
            """
    )
    ppop_zips_df = pd.read_sql("SELECT * FROM tmp_zips_df", sess.connection())
    # Get county code
    contract_df = contract_df.merge(ppop_zips_df, how="left")
    contract_df["place_of_perform_county_co"] = np.where(
        contract_df["place_of_perform_county_co"].isnull(),
        contract_df["county_code"],
        contract_df["place_of_perform_county_co"],
    )
    # Get county name
    contract_df = contract_df.merge(
        county_df,
        how="left",
        left_on=["place_of_performance_state", "place_of_perform_county_co"],
        right_on=["state_code", "county_number"],
    )
    contract_df["place_of_perform_county_na"] = np.where(
        contract_df["place_of_perform_county_na"].isnull(),
        contract_df["county_name"],
        contract_df["place_of_perform_county_na"],
    )

    del ppop_zips_df

    # Drop all ppop-based extra columns we've created through merges
    contract_df = contract_df.drop(
        [
            "country_code",
            "country_code_2_char",
            "country_name",
            "county_code",
            "county_number",
            "county_name",
            "state_code",
            "state_name",
        ],
        axis=1,
    )

    return contract_df


def calculate_legal_entity_fields(sess, contract_df, county_df, state_df, country_df, us_territories_df):
    """Calculate values that aren't in any feed (or haven't been provided properly) for legal entity

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to update with derivations
        county_df: a dataframe containing all county codes and names by state
        state_df: a dataframe containing all state codes and names
        country_df: a dataframe containing all country codes and names
        us_territories_df: a dataframe containing all the us and territory 2-char country codes, keyed by country code

    Returns:
        The contract_df dataframe with legal entity derivations
    """
    # Change US territories to the USA country code and name
    us_territories = list(us_territories_df["country_code"])
    le_us_mask = contract_df["legal_entity_country_code"].isin(us_territories)
    le_territory_mask = le_us_mask & (contract_df["legal_entity_country_code"] != "USA")

    contract_df = contract_df.merge(
        us_territories_df, how="left", left_on="legal_entity_country_code", right_on="country_code"
    ).drop("country_code", axis=1)
    contract_df["legal_entity_state_code"] = np.where(
        le_territory_mask,
        contract_df["country_code_2_char"],
        contract_df["legal_entity_state_code"],
    )
    contract_df = contract_df.merge(state_df, how="left", left_on="legal_entity_state_code", right_on="state_code")
    # updating both blank states and those that were changed due to the territory updates
    contract_df["legal_entity_state_descrip"] = np.where(
        le_territory_mask | (contract_df["legal_entity_state_descrip"].isnull()),
        contract_df["state_name"],
        contract_df["legal_entity_state_descrip"],
    )
    contract_df["legal_entity_country_name"] = np.where(
        le_territory_mask, "UNITED STATES", contract_df["legal_entity_country_name"]
    )
    contract_df["legal_entity_country_code"] = np.where(
        le_territory_mask, "USA", contract_df["legal_entity_country_code"]
    )

    # Move foreign state names from the "code" to the "description" slots
    contract_df["legal_entity_state_descrip"] = np.where(
        (contract_df["legal_entity_country_code"] != "USA") & (contract_df["legal_entity_state_code"].notnull()),
        contract_df["legal_entity_state_code"],
        contract_df["legal_entity_state_descrip"],
    )
    contract_df["legal_entity_state_code"] = np.where(
        contract_df["legal_entity_country_code"] != "USA", np.nan, contract_df["legal_entity_state_code"]
    )

    # Derive legal entity country name
    contract_df = contract_df.merge(
        country_df, how="left", left_on="legal_entity_country_code", right_on="country_code"
    )
    contract_df["legal_entity_country_name"] = np.where(
        contract_df["legal_entity_country_name"].isnull(),
        contract_df["country_name"],
        contract_df["legal_entity_country_name"],
    )

    # Drop all legal entity-based extra columns we've created through merges
    contract_df = contract_df.drop(
        ["country_code", "country_code_2_char", "country_name", "state_code", "state_name"], axis=1
    )

    le_valid_zip_mask = (contract_df["legal_entity_zip4"].str.match(r"^\d{5}(-?\d{4})?$")) & le_us_mask
    # If we have content in the zip code and it's in a valid US format, split it into 5 and 4 digit
    contract_df["legal_entity_zip5"] = np.where(le_valid_zip_mask, contract_df["legal_entity_zip4"].str[:5], np.nan)
    contract_df["legal_entity_zip_last4"] = np.where(
        le_valid_zip_mask & (contract_df["legal_entity_zip4"].str.len() > 5),
        contract_df["legal_entity_zip4"].str[-4:],
        np.nan,
    )

    # Derive le county data
    le_zips_df = contract_df[["legal_entity_zip5", "legal_entity_zip_last4"]][
        ~contract_df["legal_entity_zip5"].isnull()
    ].drop_duplicates()
    le_zips_df["legal_entity_county_code"] = np.nan
    le_zips_df.to_sql(
        "tmp_zips_df",
        con=sess.connection(),
        if_exists="replace",
        index=False,
        dtype={
            "legal_entity_zip5": types.TEXT(),
            "legal_entity_zip_last4": types.TEXT(),
            "legal_entity_county_code": types.TEXT(),
        },
    )
    # Get 9-digit-related county code
    sess.execute(
        """
            UPDATE tmp_zips_df
            SET legal_entity_county_code = county_number
            FROM zips
            WHERE legal_entity_zip5 = zip5
                AND legal_entity_zip_last4 = zip_last4;
        """
    )

    # Get 5-digit-related county code
    sess.execute(
        """
            UPDATE tmp_zips_df
            SET legal_entity_county_code = county_number
            FROM (
                SELECT zip5,
                    county_number,
                    ROW_NUMBER() OVER (PARTITION BY
                        zip5
                        ORDER BY county_number
                    ) AS row
                FROM zips_grouped
                ) duplicates
            WHERE duplicates.row = 1
                AND legal_entity_zip5 = zip5
                AND legal_entity_county_code IS NULL;
        """
    )
    le_zips_df = pd.read_sql("SELECT * FROM tmp_zips_df", sess.connection())
    # Get county code
    contract_df = contract_df.merge(le_zips_df, how="left")
    # Get county name
    contract_df = (
        contract_df.merge(
            county_df,
            how="left",
            left_on=["legal_entity_state_code", "legal_entity_county_code"],
            right_on=["state_code", "county_number"],
        )
        .drop(["state_code", "county_number"], axis=1)
        .rename(columns={"county_name": "legal_entity_county_name"})
    )
    # Delete larger unneeded DFs to save some space
    del le_zips_df

    return contract_df


def derive_transaction_unique(contract_df):
    """Derive the unique transaction key. Pulled out so it can be shared by deletes and updates

    Args:
        contract_df: dataframe to update with derivations

    Returns:
        The contract_df dataframe with completed derivations

    """
    key_list = [
        "agency_id",
        "referenced_idv_agency_iden",
        "piid",
        "award_modification_amendme",
        "parent_award_id",
        "transaction_number",
    ]
    idv_list = ["agency_id", "piid", "award_modification_amendme"]
    contract_df["detached_award_proc_unique"] = contract_df.apply(
        lambda row: "_".join(
            [
                row[key] if (row["pulled_from"] == "AWARD" or key in idv_list) and pd.notnull(row[key]) else "-none-"
                for key in key_list
            ]
        ),
        axis=1,
    )

    return contract_df


def derive_remaining_fields(
    sess, contract_df, contract_type, sub_tier_df, county_df, state_df, country_df, us_territories_df, exec_comp_df
):
    """Derive fields that aren't passed from SAM or that might be blank in their data, but we can derive them anyway

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to update with derivations
        contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
        sub_tier_df: a dataframe containing all the sub tier agency codes and their associated top tiers
        county_df: a dataframe containing all county codes and names by state
        state_df: a dataframe containing all state codes and names
        country_df: a dataframe containing all country codes and names
        us_territories_df: a dataframe containing all the us and territory 2-char country codes, keyed by country code
        exec_comp_df: a dataframe containing all the data for Executive Compensation

    Returns:
        The contract_df dataframe with completed derivations
    """
    # Calculate awarding/funding agency codes/names based on awarding/funding sub tier agency codes
    contract_df = (
        contract_df.merge(sub_tier_df, how="left", left_on="awarding_sub_tier_agency_c", right_on="sub_tier_agency_c")
        .drop("sub_tier_agency_c", axis=1)
        .rename(columns={"agency_code": "awarding_agency_code", "agency_name": "awarding_agency_name"})
    )
    contract_df = (
        contract_df.merge(sub_tier_df, how="left", left_on="funding_sub_tier_agency_co", right_on="sub_tier_agency_c")
        .drop("sub_tier_agency_c", axis=1)
        .rename(columns={"agency_code": "funding_agency_code", "agency_name": "funding_agency_name"})
    )

    # Do place of performance calculations only if we have SOME country code. If we have none at all the merge fails
    if contract_df["place_of_perform_country_c"].notnull().any():
        contract_df = calculate_ppop_fields(sess, contract_df, county_df, state_df, country_df, us_territories_df)

    # Do legal entity calculations only if we have SOME country code. If we have none at all the merge fails
    if contract_df["legal_entity_country_code"].notnull().any():
        contract_df = calculate_legal_entity_fields(
            sess, contract_df, county_df, state_df, country_df, us_territories_df
        )

    # Make sure there are no np.NaNs that could mess up the business_categories calculations
    contract_df[boolean_fields] = contract_df[boolean_fields].replace({np.NaN: "NO"})

    # Calculate business categories
    contract_df["business_categories"] = contract_df.apply(
        lambda row: get_business_categories(row=row, data_type="fpds"), axis=1
    )

    # Calculate executive compensation data for the entry. UPPER the UEI just in case it comes in lowercase
    contract_df["awardee_or_recipient_uei"] = contract_df["awardee_or_recipient_uei"].str.upper()
    contract_df = contract_df.merge(exec_comp_df, how="left", left_on="awardee_or_recipient_uei", right_on="uei").drop(
        "uei", axis=1
    )

    # Fill in 999s for all blank values in awarding/funding codes that have subtiers and add them to cgac_errors
    contract_df["awarding_agency_code"] = np.where(
        contract_df["awarding_agency_code"].isnull() & contract_df["awarding_sub_tier_agency_c"].notnull(),
        "999",
        contract_df["awarding_agency_code"],
    )
    contract_df["funding_agency_code"] = np.where(
        contract_df["funding_agency_code"].isnull() & contract_df["funding_sub_tier_agency_co"].notnull(),
        "999",
        contract_df["funding_agency_code"],
    )
    awarding_cgac_errors_df = contract_df[contract_df["awarding_agency_code"] == "999"][
        ["awarding_sub_tier_agency_c", "awarding_sub_tier_agency_n"]
    ].drop_duplicates("awarding_sub_tier_agency_c")
    funding_cgac_errors_df = contract_df[contract_df["funding_agency_code"] == "999"][
        ["funding_sub_tier_agency_co", "funding_sub_tier_agency_na"]
    ].drop_duplicates("funding_sub_tier_agency_co")
    if not awarding_cgac_errors_df.empty:
        awarding_cgac_errors_json = awarding_cgac_errors_df.set_index("awarding_sub_tier_agency_c")[
            "awarding_sub_tier_agency_n"
        ].to_dict()
        for key, value in awarding_cgac_errors_json.items():
            logger.info(
                f"WARNING: MissingSubtierCGAC: The awarding sub-tier cgac_code: {key} does not exist in cgac table. "
                f"The FPDS-provided awarding sub-tier agency name (if given) for this cgac_code is {value}. "
                "The award has been loaded with awarding_agency_code 999."
            )
            cgac_errors[key] = str(value)
    if not funding_cgac_errors_df.empty:
        funding_cgac_errors_json = funding_cgac_errors_df.set_index("funding_sub_tier_agency_co")[
            "funding_sub_tier_agency_na"
        ].to_dict()
        for key, value in funding_cgac_errors_json.items():
            logger.info(
                f"WARNING: MissingSubtierCGAC: The funding sub-tier cgac_code: {key} does not exist in cgac table. "
                f"The FPDS-provided funding sub-tier agency name (if given) for this cgac_code is {value}. "
                "The award has been loaded with funding_agency_code 999."
            )
            cgac_errors[key] = value

    del awarding_cgac_errors_df
    del funding_cgac_errors_df

    # Combine additional_reporting into one column
    contract_df["additional_reporting"] = contract_df.apply(
        lambda row: (
            "; ".join(
                [
                    row["additional_reporting_code"].split(",")[i].strip()
                    + ": "
                    + row["additional_reporting_name"].split(",")[i].strip()
                    for i in range(len(row["additional_reporting_code"].split(",")))
                ]
            )
            if pd.notnull(row["additional_reporting_code"])
            else None
        ),
        axis=1,
    )
    contract_df = contract_df.drop(["additional_reporting_code", "additional_reporting_name"], axis=1)

    # Two columns were combined in the feed. For now, just combine them and decide how to handle it later
    contract_df["vendor_legal_org_name"] = contract_df["uei_legal_business_name"]

    # Calculate the unique award key
    if contract_type == "award":
        prefix_list = ["CONT_AWD"]
        key_list = ["piid", "agency_id", "parent_award_id", "referenced_idv_agency_iden"]
    else:
        prefix_list = ["CONT_IDV"]
        key_list = ["piid", "agency_id"]

    contract_df["unique_award_key"] = contract_df.apply(
        lambda row: "_".join(
            prefix_list + [row[key] if pd.notnull(row[key]) else "-none-" for key in key_list]
        ).upper(),
        axis=1,
    )

    contract_df = derive_transaction_unique(contract_df)
    return contract_df


def process_data(
    sess, contract_df, contract_type, sub_tier_df, county_df, state_df, country_df, us_territories_df, exec_comp_df
):
    """Process the provided data by performing derivations, making sure all columns exist, and cleaning up column names
    then insert the data into the database or update existing data

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to update with derivations
        contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
        sub_tier_df: a dataframe containing all the sub tier agency codes and their associated top tiers
        county_df: a dataframe containing all county codes and names by state
        state_df: a dataframe containing all state codes and names
        country_df: a dataframe containing all country codes and names
        us_territories_df: a dataframe containing all the us and territory 2-char country codes, keyed by country code
        exec_comp_df: a dataframe containing all the data for Executive Compensation
    """
    contract_mappings = SAM_CONTRACT_MAPPINGS
    if contract_type == "IDV":
        contract_mappings = contract_mappings | IDV_MAPPINGS
    else:
        contract_mappings = contract_mappings | AWARD_MAPPINGS

    # Remove columns from dataframe that aren't in our existing mapping (we don't want them)
    contract_df = contract_df[contract_df.columns.intersection(list(contract_mappings.keys()))]

    # Add blank columns to complete the mappings for derivations and missing columns from the file
    contract_df = contract_df.reindex(
        columns=contract_df.columns.tolist() + list(contract_mappings.keys() - contract_df.columns)
    )

    # Lowercase all contract_mappings now that we've sorted through what we've gotten from the files
    contract_mappings = {k.lower(): v for k, v in contract_mappings.items()}

    contract_df = clean_data(
        contract_df,
        DetachedAwardProcurement,
        contract_mappings,
        {"place_of_perform_county_co": {"pad_to_length": 3, "keep_null": True}},
    )

    contract_df = derive_remaining_fields(
        sess, contract_df, contract_type, sub_tier_df, county_df, state_df, country_df, us_territories_df, exec_comp_df
    )

    # Insert the data
    insert_into_db(sess, contract_df)

    # Dropping tables that shouldn't remain
    sess.execute("DROP TABLE IF EXISTS tmp_zips_df")


def get_sam_contract_file(
    contract_type=None, award_type=None, delete=False, start_date=None, end_date=None, piid=None, extra_filters=None
):
    """Get the data from the atom feed based on the filters provided

    Note: This wIll simply download the file onto the running machine. It's the responsibility of the caller to
          delete the file after processing it to conserve space.

    Args:
        contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
        award_type: a string indicating what the award type of the feed being checked is
        delete: boolean representing whether to pull from the delete feed
        start_date: a date indicating the first date to pull from (must be provided with end_date)
        end_date: a date indicating the last date to pull from (must be provided with start_date)
        piid: a specific piid to filter on
        extra_filters: current dict of request filters that will be used to pull the data from the API
    """
    filename_list = ["SAM", "CONTRACT", "UPDATE" if not delete else "DELETE"]
    if contract_type:
        filename_list.append(contract_type.upper())
    if award_type:
        filename_list.append(award_type.upper())
    if start_date and end_date:
        for date_string in [start_date, end_date]:
            # convert to iso8601 for easier filename sorting
            filename_list.append(datetime.datetime.strptime(date_string, "%m/%d/%Y").strftime("%Y%m%d"))
    if piid:
        filename_list.append(f"PIID_{piid}")
    local_sam_file_name = f"{'_'.join(filename_list)}"
    local_sam_file_zipped = os.path.join(tempfile.gettempdir(), f"{local_sam_file_name}.zip")
    local_sam_file_path = os.path.join(tempfile.gettempdir(), f"{local_sam_file_name}.csv")

    filters = {
        "api_key": CONFIG_BROKER["sam"]["api_key"],
        "deletedStatus": "yes" if delete else "no",
        "format": "csv",
        "emailId": "No",
    }
    if start_date and end_date:
        filters["lastModifiedDate"] = f"[{start_date},{end_date}]"
    if contract_type:
        filters["awardOrIDV"] = contract_type
    if award_type:
        filters["awardOrIDVTypeName"] = award_type.upper()
    if piid:
        filters["piid"] = piid
    if extra_filters is not None:
        filters.update(extra_filters)

    # TODO: Refactor with load_sam_recipient.download_sam_file.
    #       Just needs to account for two separate API urls with different API contracts.

    # If the file isn't ready, it returns a 400 which already kicks off a retry after certain time (via ratelimit),
    # so we don't need to add any additional sleeping here.
    def file_ready_check(response):
        if "The specified key does not exist" in response.text:
            raise RequestException("The specified key does not exist.")

    def extract_sam_contracts_file():
        logger.info(f"Requesting file: {local_sam_file_name}")
        resp = request_sam_contracts_api(filters)
        resp_content = json.loads(resp.content.decode("utf-8"))

        if resp_content.get("presignedUrl") is None:
            logger.info(resp_content.get("message"))
            return None

        # just use the presignedUrl provided, includes the params we need (token, api key)
        logger.info(f"File requested, waiting for file to generate (token: {resp_content.get('exportToken')})")
        download_url = resp_content.get("presignedUrl").replace("REPLACE_WITH_API_KEY", CONFIG_BROKER["sam"]["api_key"])

        return request_sam_contracts_api(
            filters=None, download_url=download_url, stream=True, custom_error_check=file_ready_check
        )

    # Anytime we request a custom extract from SAM via tokens, the success rate is hit or miss to varying degrees.
    # Despite the additional backoff logic when pinging for the downloadable file, this is an attempt to retry the
    # *whole request* multiple times as sometimes it'll generate in seconds and other times it will not in an hour.
    attempt_count = 0
    max_attempts = 10
    file_content = None
    while attempt_count < max_attempts:
        try:
            file_content = extract_sam_contracts_file()
            break
        except RequestException:
            attempt_count += 1
            logger.info(f"Retrying (attempt count: {attempt_count})")
    if attempt_count == max_attempts and not file_content:
        raise RequestException(f"Couldn't generate the requested file after {attempt_count} attempts.")
    elif not file_content:
        logger.info("No data found for the requested filters.")
        return None

    logger.info("Downloading zip of request")
    try:
        with open(local_sam_file_zipped, mode="wb") as local_sam_zip:
            for chunk in file_content.iter_content(chunk_size=8192):
                if chunk:
                    local_sam_zip.write(chunk)
    finally:
        file_content.close()

    logger.info("Downloaded file. Extracting zip.")
    extracted_files = unzip(local_sam_file_zipped)
    if len(extracted_files) > 1:
        raise ValueError(f"Multiple files found in the zip: {extracted_files}")
    os.rename(os.path.join(tempfile.gettempdir(), extracted_files[0]), local_sam_file_path)
    os.remove(local_sam_file_zipped)

    logger.info(f"Extracted {local_sam_file_name}.csv")

    # Host the file in S3 for traceability
    if CONFIG_BROKER["use_aws"]:
        s3 = boto3.client("s3", region_name="us-gov-west-1")
        s3.upload_file(
            local_sam_file_path, S3_ARCHIVE, os.path.join("Contracts", os.path.basename(local_sam_file_path))
        )

    return local_sam_file_path


def process_deletes(sess, contract_df):
    """Process the deletes data coming in by adding required columns, cleaning up column names, and deleting the data.
    The only value that needs to be derived is the unique transaction key for matching against existing data.

    Args:
        sess: sqlalchemy session
        contract_df: dataframe to update with derivations

    Returns:
        dataframe of chunked data for usas to delete
    """
    delete_start = get_utc_now()
    logger.info(f"Starting delete processing at: {str(delete_start)}")
    # Remove columns from dataframe that aren't in our existing mapping (we don't want them)
    contract_df = contract_df[
        contract_df.columns.intersection(list(SAM_CONTRACT_MAPPINGS.keys()) + ["detached_award_procurement_id"])
    ]

    # Add blank columns to complete the mappings for derivations and missing columns from the file
    contract_df = contract_df.reindex(
        columns=contract_df.columns.tolist() + list(SAM_CONTRACT_MAPPINGS.keys() - contract_df.columns)
    )

    # Lowercase all contract_mappings now that we've sorted through what we've gotten from the files
    contract_mappings = {k.lower(): v for k, v in SAM_CONTRACT_MAPPINGS.items()}

    contract_df = clean_data(
        contract_df,
        DetachedAwardProcurement,
        contract_mappings,
        {},
    )

    contract_df = derive_transaction_unique(contract_df)
    contract_df["detached_award_procurement_id"] = np.nan

    # Delete the data using a temporary table
    contract_df.to_sql("tmp_contract_delete", con=sess.connection(), if_exists="replace", index=False)
    sess.execute(
        """
            UPDATE tmp_contract_delete
                SET detached_award_procurement_id = detached_award_procurement.detached_award_procurement_id
                FROM detached_award_procurement
                WHERE UPPER(detached_award_procurement.detached_award_proc_unique) =
                    UPPER(tmp_contract_delete.detached_award_proc_unique)
        """
    )
    sess.execute(
        """
            DELETE FROM detached_award_procurement USING tmp_contract_delete
            WHERE UPPER(detached_award_procurement.detached_award_proc_unique)
                    = UPPER(tmp_contract_delete.detached_award_proc_unique)
                AND detached_award_procurement.last_modified < tmp_contract_delete.last_modified
        """
    )

    # Remove the temporary table. Include IF EXISTS just in case something happened above although this should only
    # be called if there is data in the delete data anyway.
    usas_delete_data = pd.read_sql(
        "SELECT detached_award_procurement_id, detached_award_proc_unique FROM tmp_contract_delete", sess.connection()
    )
    sess.execute("DROP TABLE IF EXISTS tmp_contract_delete")
    logger.info(f"Deletes finished processing at: {str(get_utc_now())}. It took {str(get_utc_now() - delete_start)}")

    return usas_delete_data


def get_data(
    sess,
    sub_tier_df,
    county_df,
    state_df,
    country_df,
    us_territories_df,
    exec_comp_df,
    contract_type=None,
    award_type=None,
    delete=False,
    start_date=None,
    end_date=None,
    piid=None,
    extra_filters=None,
    local_file=None,
    metrics=None,
):
    """Get the data from the atom feed based on contract/award type and the last time the script was run or provided
    dates and other filters.

    Args:
        sess: the database connection
        sub_tier_df: a dataframe containing all the sub tier agency codes and their associated top tiers
        county_df: a dataframe containing all county codes and names by state
        state_df: a dataframe containing all state codes and names
        country_df: a dataframe containing all country codes and names
        us_territories_df: a dataframe containing all the us and territory 2-char country codes, keyed by country code
        exec_comp_df: a dataframe containing all the data for Executive Compensation
        contract_type: a string indicating whether the atom feed being checked is 'award' or 'IDV'
        award_type: a string indicating what the award type of the feed being checked is
        delete: boolean representing whether to pull from the delete feed
        start_date: a date indicating the first date to pull from (must be provided with end_date)
        end_date: a date indicating the last date to pull from (must be provided with start_date)
        piid: a specific piid to filter on
        extra_filters: current dict of request filters that will be used to pull the data from the API
        local_file: skip downloading the file and work from a local file if provided
        metrics: a dictionary to gather metrics for the script in
    """
    sam_contract_file = (
        local_file
        if local_file
        else get_sam_contract_file(
            contract_type=contract_type,
            award_type=award_type,
            delete=delete,
            start_date=start_date,
            end_date=end_date,
            piid=piid,
            extra_filters=extra_filters,
        )
    )
    if not sam_contract_file:
        logger.info("No file found to process data. Skipping.")
        return

    records_received = 0
    usas_delete_df = pd.DataFrame()

    specific_feed_start = get_utc_now()
    logger.info(f"Starting {contract_type}: {award_type} processing.")
    with pd.read_csv(sam_contract_file, chunksize=CHUNK_SIZE, dtype=str) as reader_obj:
        for contract_df in reader_obj:
            records_received += len(contract_df)
            if records_received >= SAM_MAX_FILE_LENGTH:
                raise Exception(f"Downloaded file contains at least {SAM_MAX_FILE_LENGTH} records. Erroring.")

            if len(contract_df) > 0:
                if not delete:
                    process_data(
                        sess,
                        contract_df,
                        contract_type,
                        sub_tier_df,
                        county_df,
                        state_df,
                        country_df,
                        us_territories_df,
                        exec_comp_df,
                    )
                else:
                    chunk_delete_df = process_deletes(sess, contract_df)
                    usas_delete_df = pd.concat([usas_delete_df, chunk_delete_df], ignore_index=True)
                logger.info(f"Successfully processed {len(contract_df)}. Total processed: {records_received}")
    logger.info(
        f"Finishing {contract_type}: {award_type} processing."
        f" It took {str(get_utc_now() - specific_feed_start)}. Total records: {records_received}."
    )

    # Remove the file after processing if remote
    if not local_file and CONFIG_BROKER["use_aws"]:
        os.remove(sam_contract_file)

    if not delete:
        metrics["records_received"] += records_received
    else:
        # Writing the file for USAS
        # TODO: Potentially update based on downstream effects for operations and USAS
        usas_delete_file = get_utc_now().strftime("%m-%d-%Y") + "_delete_records_award" + "_" + get_timestamp() + ".csv"
        metrics["deleted_{}_records_file".format(contract_type).lower()] = usas_delete_file
        usas_delete_df.to_csv(usas_delete_file, index=False)
        if CONFIG_BROKER["use_aws"]:
            s3 = boto3.client("s3", region_name=CONFIG_BROKER["aws_region"])
            # # add headers
            # contents = bytes((",".join(headers) + "\n").encode())
            # for key, value in delete_dict.items():
            #     contents += bytes("{},{}\n".format(key, value).encode())
            s3.upload_file(usas_delete_file, CONFIG_BROKER["fpds_delete_bucket"], usas_delete_file)
        os.remove(usas_delete_file)

        metrics["deletes_received"] += records_received

    sess.commit()


def create_lookups(sess):
    """Create the lookups used for contract derivations.

    Args:
        sess: connection to database

    Returns:
        Dataframes of sub tier agencies, country data, county data, state data, and executive compensation data
        including UEI number
    """

    # Get and create dataframe of sub tier agencies
    sub_tier_df = pd.read_sql(
        sess.query(
            SubTierAgency.sub_tier_agency_code.label("sub_tier_agency_c"),
            case((SubTierAgency.is_frec, FREC.frec_code), else_=CGAC.cgac_code).label("agency_code"),
            case((SubTierAgency.is_frec, FREC.agency_name), else_=CGAC.agency_name).label("agency_name"),
        )
        .join(CGAC, SubTierAgency.cgac_id == CGAC.cgac_id)
        .join(FREC, SubTierAgency.frec_id == FREC.frec_id)
        .statement,
        sess.connection(),
    )

    # Get and create dataframe of countries
    country_df = pd.read_sql(
        sess.query(CountryCode.country_code, CountryCode.country_name).statement, sess.connection()
    )
    # Get and create dataframe of us and territories
    us_territories_df = pd.read_sql(
        sess.query(CountryCode.country_code, CountryCode.country_code_2_char).filter_by(territory=True).statement,
        sess.connection(),
    )
    us_territories_df.loc[len(us_territories_df)] = ["USA", "US"]

    # Get and create dataframe of states
    state_df = pd.read_sql(
        sess.query(States.state_code, func.upper(States.state_name).label("state_name")).statement, sess.connection()
    )

    # Get and create dataframe of counties
    county_df = pd.read_sql(
        sess.query(
            CountyCode.county_number,
            CountyCode.state_code,
            func.trim(func.regexp_replace(func.upper(CountyCode.county_name), r" \(CA\)", "")).label("county_name"),
        ).statement,
        sess.connection(),
    )

    # Get and create dataframe of all exec comps and their associated UEIs
    exec_comp_df = pd.read_sql(
        sess.query(
            SAMRecipient.high_comp_officer1_full_na,
            SAMRecipient.high_comp_officer1_amount,
            SAMRecipient.high_comp_officer2_full_na,
            SAMRecipient.high_comp_officer2_amount,
            SAMRecipient.high_comp_officer3_full_na,
            SAMRecipient.high_comp_officer3_amount,
            SAMRecipient.high_comp_officer4_full_na,
            SAMRecipient.high_comp_officer4_amount,
            SAMRecipient.high_comp_officer5_full_na,
            SAMRecipient.high_comp_officer5_amount,
            func.upper(SAMRecipient.uei).label("uei"),
        )
        .filter(SAMRecipient.high_comp_officer1_full_na.isnot(None), SAMRecipient.uei.isnot(None))
        .statement,
        sess.connection(),
    )

    return sub_tier_df, country_df, us_territories_df, state_df, county_df, exec_comp_df


def main():
    sess = GlobalDB.db().session
    now = get_utc_now()

    parser = argparse.ArgumentParser(description="Pull data from SAM Contracts API.")
    parser.add_argument(
        "-f",
        "--feed",
        help="Whether to run inserts, deletes, or both",
        choices=["add", "delete", "both"],
        default="both",
    )
    parser.add_argument(
        "--start_date",
        help="Specify start date in YYYY-MM-DD format to compare to mod date. Overrides --auto option.",
        nargs=1,
        type=str,
    )
    parser.add_argument(
        "--end_date",
        help="Specify end date in YYYY-MM-DD format to compare to mod date. Inclusive. " + "Overrides --auto option.",
        nargs=1,
        type=str,
    )
    parser.add_argument(
        "--auto",
        help="Polls SAM for the latest contract data.",
        action="store_true",
    )
    parser.add_argument(
        "-p",
        "--piid",
        help="Specify specific PIID to pull",
        nargs=1,
        type=str,
    )
    parser.add_argument(
        "-l", "--local_file", type=str, default=None, help="Local filename to load. If not provided, run remotely."
    )
    args = parser.parse_args()

    award_types_award = ["Delivery Order", "BPA Call", "Definitive Contract", "Purchase Order"]
    award_types_idv = ["GWAC", "BOA", "BPA", "FSS", "IDC"]
    metrics_json = {
        "script_name": "load_sam_contract.py",
        "start_time": str(now),
        "records_received": 0,
        "deletes_received": 0,
        "deleted_award_records_file": "",
        "deleted_idv_records_file": "",
        "start_date": "",
        "end_date": "",
    }

    sub_tier_df, country_df, us_territories_df, state_df, county_df, exec_comp_df = create_lookups(sess)

    auto = args.auto
    if args.piid or args.local_file:
        start_date, end_date, auto = (None, None, False)
    else:
        start_date, end_date = validate_load_dates(
            args.start_date, args.end_date, auto, "fpds", arg_date_format="%Y-%m-%d", output_date_format="%m/%d/%Y"
        )

    if args.feed in ["add", "both"]:
        insert_start = get_utc_now()
        logger.info(f"Starting data collection at: {str(insert_start)}")

        for award_type in award_types_idv:
            get_data(
                sess,
                sub_tier_df,
                county_df,
                state_df,
                country_df,
                us_territories_df,
                exec_comp_df,
                contract_type="IDV",
                award_type=award_type,
                delete=False,
                start_date=start_date,
                end_date=end_date,
                piid=args.piid,
                local_file=args.local_file,
                metrics=metrics_json,
            )

        for award_type in award_types_award:
            get_data(
                sess,
                sub_tier_df,
                county_df,
                state_df,
                country_df,
                us_territories_df,
                exec_comp_df,
                contract_type="award",
                award_type=award_type,
                delete=False,
                start_date=start_date,
                end_date=end_date,
                piid=args.piid,
                local_file=args.local_file,
                metrics=metrics_json,
            )

        sess.commit()
        logger.info(f"Finishing data collection at: {str(get_utc_now())}. It took {str(get_utc_now() - insert_start)}")

    if args.feed in ["delete", "both"]:
        # We also need to process the delete feed
        get_data(
            sess,
            sub_tier_df,
            county_df,
            state_df,
            country_df,
            us_territories_df,
            exec_comp_df,
            delete=True,
            start_date=start_date,
            end_date=end_date,
            piid=args.piid,
            local_file=args.local_file,
            metrics=metrics_json,
        )

    # Only update load date if dates weren't specified
    if auto and args.feed == "both" and args.piid is None:
        update_external_data_load_date(now, get_utc_now(), "fpds")

    sess.commit()

    metrics_json["duration"] = str(get_utc_now() - now)

    with open("load_sam_contract_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)

    # Writing MissingSubtierCGAC error file to easily parse/manage these errors
    if cgac_errors:
        with open("cgacKeyErrors.txt", "w") as f:
            for key in cgac_errors:
                f.write("MissingSubtierCGAC: subtier_code: " + key + "; agency name: " + cgac_errors[key] + "\n")


if __name__ == "__main__":
    with create_app().app_context():
        configure_logging()
        main()
