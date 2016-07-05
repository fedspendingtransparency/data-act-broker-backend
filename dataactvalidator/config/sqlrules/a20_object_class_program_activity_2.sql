SELECT
    op.row_number,
    op.agency_identifier
FROM object_class_program_activity as op
WHERE submission_id = {}
AND op.agency_identifier IS NOT NULL
AND NOT EXISTS (SELECT cgac_id FROM cgac WHERE op.agency_identifier = cgac.cgac_code)

