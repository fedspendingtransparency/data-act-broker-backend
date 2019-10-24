-- When provided, AwardingSubTierAgencyCode must be a valid 4-character sub-tier agency code from the Federal Hierarchy.
SELECT
    row_number,
    awarding_sub_tier_agency_c,
    afa_generated_unique AS "uniqueid_afa_generated_unique"
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awarding_sub_tier_agency_c, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM sub_tier_agency AS sta
        WHERE UPPER(sta.sub_tier_agency_code) = UPPER(dafa.awarding_sub_tier_agency_c)
    )
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
