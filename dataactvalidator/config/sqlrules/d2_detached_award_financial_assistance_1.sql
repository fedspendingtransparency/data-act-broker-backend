-- The combination of fain, award modification amendment number, uri, and awarding sub tier agency code
-- must be unique unless the correction late delete indicator is C or D
SELECT
    row_number,
    fain,
    award_modification_amendme,
    uri,
    awarding_sub_tier_agency_c,
    correction_late_delete_ind
FROM (
    SELECT dafa.row_number,
        dafa.fain,
        dafa.award_modification_amendme,
        dafa.uri,
        dafa.awarding_sub_tier_agency_c,
        dafa.correction_late_delete_ind,
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