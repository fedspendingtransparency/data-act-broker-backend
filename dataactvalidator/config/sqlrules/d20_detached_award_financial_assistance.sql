-- FundingAgencyCode is an optional field, but when provided must be a valid 3-digit CGAC agency code.
SELECT
    row_number,
    funding_agency_code
FROM detached_award_financial_assistance AS dafa
WHERE submission_id = {0}
    AND funding_agency_code != ''
    AND NOT EXISTS (
        SELECT cgac_code
        FROM cgac AS cgac
        WHERE cgac.cgac_code = dafa.funding_agency_code
    )