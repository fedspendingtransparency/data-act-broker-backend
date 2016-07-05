SELECT
    op.row_number,
    op.allocation_transfer_agency
FROM object_class_program_activity as op
WHERE submission_id = {}
AND op.allocation_transfer_agency IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE op.allocation_transfer_agency = cgac.cgac_code)

