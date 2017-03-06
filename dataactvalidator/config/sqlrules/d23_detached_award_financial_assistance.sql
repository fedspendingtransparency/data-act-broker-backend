-- AwardingSubTierAgencyCode is required for all submissions and cannot be blank.
-- AwardingSubTierAgencyCode must contain a valid four character numeric code.
SELECT
    dafa.row_number,
    dafa.awarding_sub_tier_agency_c
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
    AND (dafa.awarding_sub_tier_agency_c != ''
        AND NOT EXISTS (
            SELECT sta.sub_tier_agency_code
            FROM sub_tier_agency AS sta
            WHERE sta.sub_tier_agency_code = dafa.awarding_sub_tier_agency_c)
    )