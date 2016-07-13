SELECT
    af.row_number,
    af.allocation_transfer_agency
FROM award_financial as af
WHERE submission_id = {}
AND af.allocation_transfer_agency IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE af.allocation_transfer_agency = cgac.cgac_code)

