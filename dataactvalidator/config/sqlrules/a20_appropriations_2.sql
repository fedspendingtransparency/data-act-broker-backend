SELECT
    approp.row_number,
    approp.agency_identifier
FROM appropriation as approp
WHERE submission_id = {}
AND approp.agency_identifier IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE approp.agency_identifier = cgac.cgac_code)

