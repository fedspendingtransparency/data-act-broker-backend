from collections import OrderedDict
from sqlalchemy import func, cast, Date

from dataactcore.models.stagingModels import PublishedFABS

file_model = PublishedFABS

mapping = OrderedDict(
    [
        ("afa_generated_unique", ["AssistanceTransactionUniqueKey"]),
        ("unique_award_key", ["PrimeAwardUniqueKey"]),
        ("fain", ["FAIN"]),
        ("award_modification_amendme", ["AwardModificationAmendmentNumber"]),
        ("uri", ["URI"]),
        ("sai_number", ["SAI_Number"]),
        ("funding_opportunity_number", ["FundingOpportunityNumber"]),
        ("total_funding_amount", ["TotalFundingAmount"]),
        ("federal_action_obligation", ["FederalActionObligation"]),
        ("non_federal_funding_amount", ["NonFederalFundingAmount"]),
        ("indirect_federal_sharing", ["IndirectCostFederalShareAmount"]),
        ("face_value_loan_guarantee", ["FaceValueOfDirectLoanOrLoanGuarantee"]),
        ("original_loan_subsidy_cost", ["OriginalLoanSubsidyCost"]),
        ("action_date", ["ActionDate"]),
        ("period_of_performance_star", ["PeriodOfPerformanceStartDate"]),
        ("period_of_performance_curr", ["PeriodOfPerformanceCurrentEndDate"]),
        ("awarding_agency_code", ["AwardingAgencyCode"]),
        ("awarding_agency_name", ["AwardingAgencyName"]),
        ("awarding_sub_tier_agency_c", ["AwardingSubTierAgencyCode"]),
        ("awarding_sub_tier_agency_n", ["AwardingSubTierAgencyName"]),
        ("awarding_office_code", ["AwardingOfficeCode"]),
        ("awarding_office_name", ["AwardingOfficeName"]),
        ("funding_agency_code", ["FundingAgencyCode"]),
        ("funding_agency_name", ["FundingAgencyName"]),
        ("funding_sub_tier_agency_co", ["FundingSubTierAgencyCode"]),
        ("funding_sub_tier_agency_na", ["FundingSubTierAgencyName"]),
        ("funding_office_code", ["FundingOfficeCode"]),
        ("funding_office_name", ["FundingOfficeName"]),
        ("uei", ["AwardeeOrRecipientUEI"]),
        ("awardee_or_recipient_legal", ["AwardeeOrRecipientLegalEntityName"]),
        ("ultimate_parent_uei", ["UltimateParentUEI"]),
        ("ultimate_parent_legal_enti", ["UltimateParentLegalEntityName"]),
        ("legal_entity_country_code", ["LegalEntityCountryCode"]),
        ("legal_entity_country_name", ["LegalEntityCountryName"]),
        ("legal_entity_address_line1", ["LegalEntityAddressLine1"]),
        ("legal_entity_address_line2", ["LegalEntityAddressLine2"]),
        ("legal_entity_city_code", ["LegalEntityCityCode"]),
        ("legal_entity_city_name", ["LegalEntityCityName"]),
        ("legal_entity_state_code", ["LegalEntityStateCode"]),
        ("legal_entity_state_name", ["LegalEntityStateName"]),
        ("legal_entity_zip5", ["LegalEntityZIP5"]),
        ("legal_entity_zip_last4", ["LegalEntityZIPLast4"]),
        ("legal_entity_county_code", ["LegalEntityCountyCode"]),
        ("legal_entity_county_name", ["LegalEntityCountyName"]),
        ("legal_entity_congressional", ["LegalEntityCongressionalDistrict"]),
        ("legal_entity_foreign_city", ["LegalEntityForeignCityName"]),
        ("legal_entity_foreign_provi", ["LegalEntityForeignProvinceName"]),
        ("legal_entity_foreign_posta", ["LegalEntityForeignPostalCode"]),
        ("place_of_performance_code", ["PrimaryPlaceOfPerformanceCode"]),
        ("place_of_performance_scope", ["PrimaryPlaceOfPerformanceScope"]),
        ("place_of_performance_city", ["PrimaryPlaceOfPerformanceCityName"]),
        ("place_of_perform_county_co", ["PrimaryPlaceOfPerformanceCountyCode"]),
        ("place_of_perform_county_na", ["PrimaryPlaceOfPerformanceCountyName"]),
        ("place_of_perform_state_nam", ["PrimaryPlaceOfPerformanceStateName"]),
        ("place_of_performance_zip4a", ["PrimaryPlaceOfPerformanceZIP+4"]),
        ("place_of_performance_congr", ["PrimaryPlaceOfPerformanceCongressionalDistrict"]),
        ("place_of_perform_country_c", ["PrimaryPlaceOfPerformanceCountryCode"]),
        ("place_of_perform_country_n", ["PrimaryPlaceOfPerformanceCountryName"]),
        ("place_of_performance_forei", ["PrimaryPlaceOfPerformanceForeignLocationDescription"]),
        ("assistance_listing_number", ["AssistanceListingNumber"]),
        ("assistance_listing_title", ["AssistanceListingTitle"]),
        ("assistance_type", ["AssistanceType"]),
        ("assistance_type_desc", ["AssistanceTypeDescriptionTag"]),
        ("award_description", ["AwardDescription"]),
        ("funding_opportunity_goals", ["FundingOpportunityGoalsText"]),
        ("business_funds_indicator", ["BusinessFundsIndicator"]),
        ("business_funds_ind_desc", ["BusinessFundsIndicatorDescriptionTag"]),
        ("business_types", ["BusinessTypes"]),
        ("business_types_desc", ["BusinessTypesDescriptionTag"]),
        ("correction_delete_indicatr", ["CorrectionDeleteIndicator"]),
        ("correction_delete_ind_desc", ["CorrectionDeleteIndicatorDescriptionTag"]),
        ("action_type", ["ActionType"]),
        ("action_type_description", ["ActionTypeDescriptionTag"]),
        ("record_type", ["RecordType"]),
        ("record_type_description", ["RecordTypeDescriptionTag"]),
        ("modified_at", ["LastModifiedDate"]),
    ]
)
db_columns = [key for key in mapping]


def query_data(session, agency_code, agency_type, start, end):
    """Request D2 file data

    Args:
        session: DB session
        agency_code: FREC or CGAC code for generation
        agency_type: The type of agency (awarding or funding) to generate the file for
        start: Beginning of period for D file
        end: End of period for D file

    Returns:
        The rows using the provided dates for the given agency.
    """
    rows = (
        initial_query(session)
        .filter(file_model.is_active.is_(True))
        .filter(func.cast_as_date(file_model.action_date) >= start)
        .filter(func.cast_as_date(file_model.action_date) <= end)
    )

    # Funding or awarding agency filtering
    if agency_type == "funding":
        rows = rows.filter(file_model.funding_agency_code == agency_code)
    else:
        rows = rows.filter(file_model.awarding_agency_code == agency_code)

    return rows


def query_published_fabs_data(session, submission_id):
    """Request published FABS file data

    Args:
        session: DB session
        submission_id: Submission ID for generation

    Returns:
        A query to gather published data from the provided submission
    """
    return initial_query(session).filter(file_model.submission_id == submission_id)


def initial_query(session):
    """Creates the initial query for D2 files.

    Args:
        session: The current DB session

    Returns:
        The base query (a select from the PublishedFABS table with the specified columns).
    """
    return session.query(
        file_model.afa_generated_unique,
        file_model.unique_award_key,
        file_model.fain,
        file_model.award_modification_amendme,
        file_model.uri,
        file_model.sai_number,
        file_model.funding_opportunity_number,
        file_model.total_funding_amount,
        file_model.federal_action_obligation,
        file_model.non_federal_funding_amount,
        file_model.indirect_federal_sharing,
        file_model.face_value_loan_guarantee,
        file_model.original_loan_subsidy_cost,
        func.to_char(cast(file_model.action_date, Date), "YYYYMMDD"),
        func.to_char(cast(file_model.period_of_performance_star, Date), "YYYYMMDD"),
        func.to_char(cast(file_model.period_of_performance_curr, Date), "YYYYMMDD"),
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
        file_model.uei,
        file_model.awardee_or_recipient_legal,
        file_model.ultimate_parent_uei,
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
        file_model.assistance_listing_number,
        file_model.assistance_listing_title,
        file_model.assistance_type,
        file_model.assistance_type_desc,
        file_model.award_description,
        file_model.funding_opportunity_goals,
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
        func.to_char(cast(file_model.modified_at, Date), "YYYYMMDD"),
    )
