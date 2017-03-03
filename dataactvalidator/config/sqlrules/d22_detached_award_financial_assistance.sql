-- AwardingAgencyCode is optional. When provided must be a valid 3-digit CGAC agency code.
-- If the AwardingAgencyCode is blank, it is auto-populated from the AwardingSubTierAgencyCode.
SELECT
    row_number,
    awarding_agency_code,
    awarding_sub_tier_agency_c
FROM detached_award_financial_assistance as dafa
WHERE submission_id = {0}
    AND (awarding_agency_code != ''
         AND NOT EXISTS (
            SELECT cgac_code
            FROM cgac
            WHERE cgac_code = dafa.awarding_agency_code))
    OR (awarding_agency_code =''
        AND (awarding_agency_code != awarding_sub_tier_agency_c))