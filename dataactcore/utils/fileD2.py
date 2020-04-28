from collections import OrderedDict
from sqlalchemy import func, cast, Date

from dataactcore.models.stagingModels import PublishedAwardFinancialAssistance

file_model = PublishedAwardFinancialAssistance

mapping = OrderedDict([
    ('PrimeAwardUniqueKey', 'unique_award_key'),
    ('AssistanceTransactionUniqueKey', 'afa_generated_unique'),
    ('FAIN', 'fain'),
    ('AwardModificationAmendmentNumber', 'award_modification_amendme'),
    ('URI', 'uri'),
    ('SAI_Number', 'sai_number'),
    ('TotalFundingAmount', 'total_funding_amount'),
    ('FederalActionObligation', 'federal_action_obligation'),
    ('NonFederalFundingAmount', 'non_federal_funding_amount'),
    ('FaceValueOfDirectLoanOrLoanGuarantee', 'face_value_loan_guarantee'),
    ('OriginalLoanSubsidyCost', 'original_loan_subsidy_cost'),
    ('ActionDate', 'action_date'),
    ('PeriodOfPerformanceStartDate', 'period_of_performance_star'),
    ('PeriodOfPerformanceCurrentEndDate', 'period_of_performance_curr'),
    ('AwardingAgencyCode', 'awarding_agency_code'),
    ('AwardingAgencyName', 'awarding_agency_name'),
    ('AwardingSubTierAgencyCode', 'awarding_sub_tier_agency_c'),
    ('AwardingSubTierAgencyName', 'awarding_sub_tier_agency_n'),
    ('AwardingOfficeCode', 'awarding_office_code'),
    ('AwardingOfficeName', 'awarding_office_name'),
    ('FundingAgencyCode', 'funding_agency_code'),
    ('FundingAgencyName', 'funding_agency_name'),
    ('FundingSubTierAgencyCode', 'funding_sub_tier_agency_co'),
    ('FundingSubTierAgencyName', 'funding_sub_tier_agency_na'),
    ('FundingOfficeCode', 'funding_office_code'),
    ('FundingOfficeName', 'funding_office_name'),
    ('AwardeeOrRecipientUniqueIdentifier', 'awardee_or_recipient_uniqu'),
    ('AwardeeOrRecipientLegalEntityName', 'awardee_or_recipient_legal'),
    ('UltimateParentUniqueIdentifier', 'ultimate_parent_unique_ide'),
    ('UltimateParentLegalEntityName', 'ultimate_parent_legal_enti'),
    ('LegalEntityCountryCode', 'legal_entity_country_code'),
    ('LegalEntityCountryName', 'legal_entity_country_name'),
    ('LegalEntityAddressLine1', 'legal_entity_address_line1'),
    ('LegalEntityAddressLine2', 'legal_entity_address_line2'),
    ('LegalEntityCityCode', 'legal_entity_city_code'),
    ('LegalEntityCityName', 'legal_entity_city_name'),
    ('LegalEntityStateCode', 'legal_entity_state_code'),
    ('LegalEntityStateName', 'legal_entity_state_name'),
    ('LegalEntityZIP5', 'legal_entity_zip5'),
    ('LegalEntityZIPLast4', 'legal_entity_zip_last4'),
    ('LegalEntityCountyCode', 'legal_entity_county_code'),
    ('LegalEntityCountyName', 'legal_entity_county_name'),
    ('LegalEntityCongressionalDistrict', 'legal_entity_congressional'),
    ('LegalEntityForeignCityName', 'legal_entity_foreign_city'),
    ('LegalEntityForeignProvinceName', 'legal_entity_foreign_provi'),
    ('LegalEntityForeignPostalCode', 'legal_entity_foreign_posta'),
    ('PrimaryPlaceOfPerformanceCode', 'place_of_performance_code'),
    ('PrimaryPlaceOfPerformanceScope', 'place_of_performance_scope'),
    ('PrimaryPlaceOfPerformanceCityName', 'place_of_performance_city'),
    ('PrimaryPlaceOfPerformanceCountyCode', 'place_of_perform_county_co'),
    ('PrimaryPlaceOfPerformanceCountyName', 'place_of_perform_county_na'),
    ('PrimaryPlaceOfPerformanceStateName', 'place_of_perform_state_nam'),
    ('PrimaryPlaceOfPerformanceZIP+4', 'place_of_performance_zip4a'),
    ('PrimaryPlaceOfPerformanceCongressionalDistrict', 'place_of_performance_congr'),
    ('PrimaryPlaceOfPerformanceCountryCode', 'place_of_perform_country_c'),
    ('PrimaryPlaceOfPerformanceCountryName', 'place_of_perform_country_n'),
    ('PrimaryPlaceOfPerformanceForeignLocationDescription', 'place_of_performance_forei'),
    ('CFDA_Number', 'cfda_number'),
    ('CFDA_Title', 'cfda_title'),
    ('AssistanceType', 'assistance_type'),
    ('AssistanceTypeDescriptionTag', 'assistance_type_desc'),
    ('AwardDescription', 'award_description'),
    ('BusinessFundsIndicator', 'business_funds_indicator'),
    ('BusinessFundsIndicatorDescriptionTag', 'business_funds_ind_desc'),
    ('BusinessTypes', 'business_types'),
    ('BusinessTypesDescriptionTag', 'business_types_desc'),
    ('CorrectionDeleteIndicator', 'correction_delete_indicatr'),
    ('CorrectionDeleteIndicatorDescriptionTag', 'correction_delete_ind_desc'),
    ('ActionType', 'action_type'),
    ('ActionTypeDescriptionTag', 'action_type_description'),
    ('RecordType', 'record_type'),
    ('RecordTypeDescriptionTag', 'record_type_description'),
    ('LastModifiedDate', 'modified_at')
])
db_columns = [val for key, val in mapping.items()]


def query_data(session, agency_code, agency_type, start, end):
    """ Request D2 file data

        Args:
            session: DB session
            agency_code: FREC or CGAC code for generation
            agency_type: The type of agency (awarding or funding) to generate the file for
            start: Beginning of period for D file
            end: End of period for D file

        Returns:
            The rows using the provided dates for the given agency.
    """
    rows = initial_query(session).\
        filter(file_model.is_active.is_(True)).\
        filter(func.cast_as_date(file_model.action_date) >= start).\
        filter(func.cast_as_date(file_model.action_date) <= end)

    # Funding or awarding agency filtering
    if agency_type == 'funding':
        rows = rows.filter(file_model.funding_agency_code == agency_code)
    else:
        rows = rows.filter(file_model.awarding_agency_code == agency_code)

    return rows


def query_published_fabs_data(session, submission_id):
    """ Request published FABS file data

        Args:
            session: DB session
            submission_id: Submission ID for generation

        Returns:
            A query to gather published data from the provided submission
    """
    return initial_query(session).filter(file_model.submission_id == submission_id)


def initial_query(session):
    """ Creates the initial query for D2 files.

        Args:
            session: The current DB session

        Returns:
            The base query (a select from the PublishedAwardFinancialAssistance table with the specified columns).
    """
    return session.query(
        file_model.unique_award_key,
        file_model.afa_generated_unique,
        file_model.fain,
        file_model.award_modification_amendme,
        file_model.uri,
        file_model.sai_number,
        file_model.total_funding_amount,
        file_model.federal_action_obligation,
        file_model.non_federal_funding_amount,
        file_model.face_value_loan_guarantee,
        file_model.original_loan_subsidy_cost,
        func.to_char(cast(file_model.action_date, Date), 'YYYYMMDD'),
        func.to_char(cast(file_model.period_of_performance_star, Date), 'YYYYMMDD'),
        func.to_char(cast(file_model.period_of_performance_curr, Date), 'YYYYMMDD'),
        file_model.awarding_agency_code,
        file_model.awarding_agency_name,
        file_model.awarding_sub_tier_agency_c,
        file_model.awarding_sub_tier_agency_n,
        file_model.awarding_office_code,
        file_model.awarding_office_name,
        file_model.funding_agency_code,
        file_model.funding_agency_name,
        file_model.funding_sub_tier_agency_co,
        file_model.funding_sub_tier_agency_na,
        file_model.funding_office_code,
        file_model.funding_office_name,
        file_model.awardee_or_recipient_uniqu,
        file_model.awardee_or_recipient_legal,
        file_model.ultimate_parent_unique_ide,
        file_model.ultimate_parent_legal_enti,
        file_model.legal_entity_country_code,
        file_model.legal_entity_country_name,
        file_model.legal_entity_address_line1,
        file_model.legal_entity_address_line2,
        file_model.legal_entity_city_code,
        file_model.legal_entity_city_name,
        file_model.legal_entity_state_code,
        file_model.legal_entity_state_name,
        file_model.legal_entity_zip5,
        file_model.legal_entity_zip_last4,
        file_model.legal_entity_county_code,
        file_model.legal_entity_county_name,
        file_model.legal_entity_congressional,
        file_model.legal_entity_foreign_city,
        file_model.legal_entity_foreign_provi,
        file_model.legal_entity_foreign_posta,
        file_model.place_of_performance_code,
        file_model.place_of_performance_scope,
        file_model.place_of_performance_city,
        file_model.place_of_perform_county_co,
        file_model.place_of_perform_county_na,
        file_model.place_of_perform_state_nam,
        file_model.place_of_performance_zip4a,
        file_model.place_of_performance_congr,
        file_model.place_of_perform_country_c,
        file_model.place_of_perform_country_n,
        file_model.place_of_performance_forei,
        file_model.cfda_number,
        file_model.cfda_title,
        file_model.assistance_type,
        file_model.assistance_type_desc,
        file_model.award_description,
        file_model.business_funds_indicator,
        file_model.business_funds_ind_desc,
        file_model.business_types,
        file_model.business_types_desc,
        file_model.correction_delete_indicatr,
        file_model.correction_delete_ind_desc,
        file_model.action_type,
        file_model.action_type_description,
        file_model.record_type,
        file_model.record_type_description,
        func.to_char(cast(file_model.modified_at, Date), 'YYYYMMDD'))
