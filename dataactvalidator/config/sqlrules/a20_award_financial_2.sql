SELECT
    af.row_number,
    af.agency_identifier
FROM award_financial as af
WHERE submission_id = {}
AND af.agency_identifier IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE af.agency_identifier = cgac.cgac_code)

