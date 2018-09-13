-- When provided, AwardingSubTierAgencyCode must be a valid 4-character sub-tier agency code from the Federal Hierarchy.
SELECT
    row_number,
    awarding_sub_tier_agency_c
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND COALESCE(awarding_sub_tier_agency_c, '') <> ''
    AND NOT EXISTS (
        SELECT 1
        FROM sub_tier_agency AS sta
        WHERE sta.sub_tier_agency_code = dafa.awarding_sub_tier_agency_c
    );
