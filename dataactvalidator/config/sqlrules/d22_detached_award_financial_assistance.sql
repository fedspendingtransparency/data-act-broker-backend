-- AwardingAgencyCode must be a valid 3-digit CGAC agency code.
SELECT
    dafa.row_number,
    dafa.awarding_agency_code
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
    AND NOT EXISTS (
        SELECT cgac.cgac_code
        FROM cgac AS cgac
        WHERE cgac.cgac_code = dafa.awarding_agency_code
    )