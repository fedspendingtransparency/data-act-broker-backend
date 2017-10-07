from collections import OrderedDict
from sqlalchemy import func, cast, Date

from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance, AwardFinancialAssistance

file_model = PublishedAwardFinancialAssistance
staging_model = AwardFinancialAssistance

mapping = OrderedDict([
    ('actiontype', 'action_type'),
    ('actiondate', 'action_date'),
    ('assistancetype', 'assistance_type'),
    ('recordtype', 'record_type'),
    ('fain', 'fain'),
    ('awardmodificationamendmentnumber', 'award_modification_amendme'),
    ('uri', 'uri'),
    ('correctionlatedeleteindicator', 'correction_late_delete_ind'),
    ('fiscalyearandquartercorrection', 'fiscal_year_and_quarter_co'),
    ('sai_number', 'sai_number'),
    ('awardeeorrecipientlegalentityname', 'awardee_or_recipient_legal'),
    ('awardeeorrecipientuniqueidentifier', 'awardee_or_recipient_uniqu'),
    ('legalentityaddressline1', 'legal_entity_address_line1'),
    ('legalentityaddressline2', 'legal_entity_address_line2'),
    ('legalentityaddressline3', 'legal_entity_address_line3'),
    ('legalentitycityname', 'legal_entity_city_name'),
    ('legalentitycitycode', 'legal_entity_city_code'),
    ('legalentitycountyname', 'legal_entity_county_name'),
    ('legalentitycountycode', 'legal_entity_county_code'),
    ('legalentitycountryname', 'legal_entity_country_name'),
    ('legalentitystatename', 'legal_entity_state_name'),
    ('legalentitystatecode', 'legal_entity_state_code'),
    ('legalentityzip5', 'legal_entity_zip5'),
    ('legalentityziplast4', 'legal_entity_zip_last4'),
    ('legalentitycountrycode', 'legal_entity_country_code'),
    ('legalentityforeigncityname', 'legal_entity_foreign_city'),
    ('legalentityforeignprovincename', 'legal_entity_foreign_provi'),
    ('legalentityforeignpostalcode', 'legal_entity_foreign_posta'),
    ('legalentitycongressionaldistrict', 'legal_entity_congressional'),
    ('businesstypes', 'business_types'),
    ('fundingagencyname', 'funding_agency_name'),
    ('fundingagencycode', 'funding_agency_code'),
    ('fundingsubtieragencyname', 'funding_sub_tier_agency_na'),
    ('fundingsubtieragencycode', 'funding_sub_tier_agency_co'),
    ('fundingofficecode', 'funding_office_code'),
    ('awardingagencyname', 'awarding_agency_name'),
    ('awardingagencycode', 'awarding_agency_code'),
    ('awardingsubtieragencyname', 'awarding_sub_tier_agency_n'),
    ('awardingsubtieragencycode', 'awarding_sub_tier_agency_c'),
    ('awardingofficename', 'awarding_office_name'),
    ('awardingofficecode', 'awarding_office_code'),
    ('cfda_number', 'cfda_number'),
    ('cfda_title', 'cfda_title'),
    ('primaryplaceofperformancecode', 'place_of_performance_code'),
    ('primaryplaceofperformancecountrycode', 'place_of_perform_country_c'),
    ('primaryplaceofperformancecountryname', 'place_of_perform_country_n'),
    ('primaryplaceofperformancecountycode', 'place_of_perform_county_co'),
    ('primaryplaceofperformancestatename', 'place_of_perform_state_nam'),
    ('primaryplaceofperformancecountyname', 'place_of_perform_county_na'),
    ('primaryplaceofperformancecityname', 'place_of_performance_city'),
    ('primaryplaceofperformancezip_4', 'place_of_performance_zip4a'),
    ('primaryplaceofperformanceforeignlocationdescription', 'place_of_performance_forei'),
    ('primaryplaceofperformancecongressionaldistrict', 'place_of_performance_congr'),
    ('awarddescription', 'award_description'),
    ('periodofperformancestartdate', 'period_of_performance_star'),
    ('periodofperformancecurrentenddate', 'period_of_performance_curr'),
    ('federalactionobligation', 'federal_action_obligation'),
    ('nonfederalfundingamount', 'non_federal_funding_amount'),
    ('totalfundingamount', 'total_funding_amount'),
    ('facevalueofdirectloanorloanguarantee', 'face_value_loan_guarantee'),
    ('originalloansubsidycost', 'original_loan_subsidy_cost'),
    ('businessfundsindicator', 'business_funds_indicator'),
    ('fundingofficename', 'funding_office_name'),
    ('lastmodifieddate', 'modified_at')
])
db_columns = [val for key, val in mapping.items()]


def query_data(session, agency_code, start, end, page_start, page_stop):
    """ Request D2 file data

        Args:
            session - DB session
            agency_code - FREC or CGAC code for generation
            start - Beginning of period for D file
            end - End of period for D file
            page_start - Beginning of pagination
            page_stop - End of pagination
    """
    rows = session.query(
        file_model.action_type,
        func.to_char(cast(file_model.action_date, Date), 'YYYYMMDD'),
        file_model.assistance_type,
        file_model.record_type,
        file_model.fain,
        file_model.award_modification_amendme,
        file_model.uri,
        file_model.correction_late_delete_ind,
        file_model.fiscal_year_and_quarter_co,
        file_model.sai_number,
        file_model.awardee_or_recipient_legal,
        file_model.awardee_or_recipient_uniqu,
        file_model.legal_entity_address_line1,
        file_model.legal_entity_address_line2,
        file_model.legal_entity_address_line3,
        file_model.legal_entity_city_name,
        file_model.legal_entity_city_code,
        file_model.legal_entity_county_name,
        file_model.legal_entity_county_code,
        file_model.legal_entity_country_name,
        file_model.legal_entity_state_name,
        file_model.legal_entity_state_code,
        file_model.legal_entity_zip5,
        file_model.legal_entity_zip_last4,
        file_model.legal_entity_country_code,
        file_model.legal_entity_foreign_city,
        file_model.legal_entity_foreign_provi,
        file_model.legal_entity_foreign_posta,
        file_model.legal_entity_congressional,
        file_model.business_types,
        file_model.funding_agency_name,
        file_model.funding_agency_code,
        file_model.funding_sub_tier_agency_na,
        file_model.funding_sub_tier_agency_co,
        file_model.funding_office_code,
        file_model.awarding_agency_name,
        file_model.awarding_agency_code,
        file_model.awarding_sub_tier_agency_n,
        file_model.awarding_sub_tier_agency_c,
        file_model.awarding_office_name,
        file_model.awarding_office_code,
        file_model.cfda_number,
        file_model.cfda_title,
        file_model.place_of_performance_code,
        file_model.place_of_perform_country_c,
        file_model.place_of_perform_country_n,
        file_model.place_of_perform_county_co,
        file_model.place_of_perform_state_nam,
        file_model.place_of_perform_county_na,
        file_model.place_of_performance_city,
        file_model.place_of_performance_zip4a,
        file_model.place_of_performance_forei,
        file_model.place_of_performance_congr,
        file_model.award_description,
        func.to_char(cast(file_model.period_of_performance_star, Date), 'YYYYMMDD'),
        func.to_char(cast(file_model.period_of_performance_curr, Date), 'YYYYMMDD'),
        file_model.federal_action_obligation,
        file_model.non_federal_funding_amount,
        file_model.total_funding_amount,
        file_model.face_value_loan_guarantee,
        file_model.original_loan_subsidy_cost,
        file_model.business_funds_indicator,
        file_model.funding_office_name,
        func.to_char(cast(file_model.modified_at, Date), 'YYYYMMDD')).\
        filter(file_model.is_active.is_(True)).\
        filter(file_model.awarding_agency_code == agency_code).\
        filter(cast(file_model.action_date, Date) >= start).\
        filter(cast(file_model.action_date, Date) <= end).\
        slice(page_start, page_stop)
    return rows
