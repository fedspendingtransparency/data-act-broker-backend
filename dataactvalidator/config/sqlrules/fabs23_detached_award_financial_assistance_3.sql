-- AwardingSubTierAgencyCode must be provided when AwardingOfficeCode is not provided.
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    awarding_office_code,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(awarding_sub_tier_agency_c, '') = ''
    AND COALESCE(awarding_office_code, '') = ''
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
