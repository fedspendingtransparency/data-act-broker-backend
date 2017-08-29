from collections import OrderedDict
from sqlalchemy import cast, Date

from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

file_model = PublishedAwardFinancialAssistance

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
    # ('legalentitycitycode', 'legal_entity_city_code'),  # This isn't in the PublishedAwardFinancialAssistance table
    ('legalentitycountyname', 'legal_entity_county_name'),
    ('legalentitycountycode', 'legal_entity_county_code'),
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
    # ('awardingofficename', 'awarding_office_name'),  # This isn't in the PublishedAwardFinancialAssistance table
    ('awardingofficecode', 'awarding_office_code'),
    ('cfda_number', 'cfda_number'),
    ('cfda_title', 'cfda_title'),
    ('primaryplaceofperformancecode', 'place_of_performance_code'),
    ('primaryplaceofperformancecountrycode', 'place_of_perform_country_c'),
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
    ('facevalueloanguarantee', 'face_value_loan_guarantee'),
    ('originalloansubsidycost', 'original_loan_subsidy_cost'),
    ('businessfundsindicator', 'business_funds_indicator'),
    # ('submissiontype', 'submissiontype'),  # This isn't in the PublishedAwardFinancialAssistance table
    # ('fundingofficename', 'fundingofficename'),  # This isn't in the PublishedAwardFinancialAssistance table
    ('lastmodifieddate', 'modified_at')
])


def query_data(session, agency_code, start, end):
    rows = session.query(file_model).\
        filter(file_model.is_active == 'True').\
        filter(file_model.awarding_agency_code == agency_code).\
        filter(cast(file_model.action_date, Date) >= start).\
        filter(cast(file_model.action_date, Date) <= end)
    session.commit()
    return rows
