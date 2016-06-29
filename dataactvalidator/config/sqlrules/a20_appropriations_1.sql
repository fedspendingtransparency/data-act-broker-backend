SELECT
    approp.row_number,
    approp.allocation_transfer_agency
FROM appropriation as approp
WHERE submission_id = {}
AND approp.allocation_transfer_agency IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE approp.allocation_transfer_agency = cgac.cgac_code)

