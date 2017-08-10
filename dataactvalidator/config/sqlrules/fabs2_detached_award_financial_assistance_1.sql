-- The combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must be unique
-- within the submission file.
SELECT
    row_number,
    fain,
    award_modification_amendme,
    uri,
    awarding_sub_tier_agency_c
FROM (
    SELECT dafa.row_number,
        dafa.fain,
        dafa.award_modification_amendme,
        dafa.uri,
        dafa.awarding_sub_tier_agency_c,
        ROW_NUMBER() OVER (PARTITION BY
            dafa.fain,
            dafa.award_modification_amendme,
            dafa.uri,
            dafa.awarding_sub_tier_agency_c
        ) AS row
    FROM detached_award_financial_assistance as dafa
    WHERE dafa.submission_id = {0}
    ORDER BY dafa.row_number
    ) duplicates
WHERE duplicates.row > 1