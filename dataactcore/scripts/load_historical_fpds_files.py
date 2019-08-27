import argparse
import urllib.request
import datetime
import csv
import io
import os
import boto3
import re
import zipfile
import numpy as np
import pandas as pd

import logging

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactcore.models.domainModels import SubTierAgency
from dataactcore.models.stagingModels import DetachedAwardProcurement
from dataactcore.models.jobModels import Submission  # noqa
from dataactcore.models.userModel import User  # noqa

from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


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
    data['unique_award_key'] = data.apply(lambda x: create_unique_award_key(x), axis=1)

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
            'unique_award_key': 'unique_award_key',
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
    idv_list = ['agencyid', 'piid', 'modnumber']
    unique_string = ""
    for item in key_list:
        if len(unique_string) > 0:
            unique_string += "_"
        if row[item] and str(row[item]) != 'nan' and (row['pulled_from'] == 'award' or item in idv_list):
            unique_string += str(row[item])
        else:
            unique_string += "-none-"
    return unique_string


def create_unique_award_key(row):
    key_list = ['piid', 'agencyid', 'idvpiid', 'idvagencyid'] if row['pulled_from'] == 'award' else ['piid', 'agencyid']
    unique_string_list = ['CONT_AWD'] if row['pulled_from'] == 'award' else ['CONT_IDV']

    for item in key_list:
        unique_string_list.append(row[item] if row[item] and str(row[item]) != 'nan' else '-none-')

    return '_'.join(unique_string_list).upper()


def main():
    sess = GlobalDB.db().session

    parser = argparse.ArgumentParser(description='Pull data from the FPDS Atom Feed.')
    parser.add_argument('-sf', '--subfolder', help='Used to indicate which Subfolder to load files from', nargs="+",
                        type=str)
    args = parser.parse_args()

    # get and create list of sub tier agencies
    sub_tiers = sess.query(SubTierAgency).all()
    sub_tier_list = {}

    for sub_tier in sub_tiers:
        sub_tier_list[sub_tier.sub_tier_agency_code] = sub_tier

    max_year = 2015
    subfolder = None

    if args.subfolder:
        if len(args.subfolder) != 1:
            logger.error("When using the -sf flag, please enter just one string for the folder name")
            raise ValueError("When using the -sf flag, please enter just one string for the folder name")
        subfolder = args.subfolder[0]

    logger.info("Starting file loads at: %s", str(datetime.datetime.now()))
    if CONFIG_BROKER["use_aws"]:
        # # get naics dictionary
        s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
        agency_list_path = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                           'Key': "naics.csv"}, ExpiresIn=600)
        agency_list_file = urllib.request.urlopen(agency_list_path)
        reader = csv.reader(agency_list_file.read().decode("utf-8").splitlines())
        naics_dict = {rows[0]: rows[1].upper() for rows in reader}

        # Gather list of files
        if subfolder:
            subfolder = subfolder + "/"
            file_list = s3_client.list_objects_v2(Bucket=CONFIG_BROKER['archive_bucket'], Prefix=subfolder)
        else:
            file_list = s3_client.list_objects_v2(Bucket=CONFIG_BROKER['archive_bucket'])
        # Parse files
        for obj in file_list.get('Contents', []):
            match_string = '^\d{4}_All_Contracts_Full_\d{8}.csv.zip'
            if subfolder:
                match_string = "^" + subfolder + "\d{4}_All_Contracts_Full_\d{8}.csv.zip"
            if re.match(match_string, obj['Key']):
                # we only want up through 2015 for this data unless it’s a subfolder, then do all of them
                if subfolder or int(obj['Key'][:4]) <= max_year:
                    s3_res = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
                    s3_object = s3_res.Object(CONFIG_BROKER['archive_bucket'], obj['Key'])
                    response = s3_object.get()
                    pa_file = io.BytesIO(response['Body'].read())
                    # Parse the file
                    parse_fpds_file(pa_file, sess, sub_tier_list, naics_dict, filename=obj['Key'])
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

    sess.commit()
    logger.info("Ending at: %s", str(datetime.datetime.now()))


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
