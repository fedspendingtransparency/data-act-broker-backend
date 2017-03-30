WITH object_class_program_activity_b9_{0} AS
    (SELECT tas,
        submission_id,
        row_number,
        agency_identifier,
        allocation_transfer_agency,
        main_account_code,
        program_activity_name,
        program_activity_code
    FROM object_class_program_activity
    WHERE submission_id = {0})
SELECT  op.tas,
    op.submission_id,
    op.row_number,
	op.agency_identifier,
	op.allocation_transfer_agency,
	op.main_account_code,
	op.program_activity_name,
	op.program_activity_code
FROM object_class_program_activity_b9_{0} as op
    INNER JOIN submission as sub
        ON op.submission_id = sub.submission_id
WHERE op.submission_id = {0}
    AND op.program_activity_code <> '0000'
    AND LOWER(op.program_activity_name) <> 'unknown/other'
	AND op.row_number NOT IN (
		SELECT op.row_number
		FROM object_class_program_activity_b9_{0} as op
			JOIN program_activity as pa
				ON (op.agency_identifier IS NOT DISTINCT FROM pa.agency_id
				AND op.allocation_transfer_agency IS NOT DISTINCT FROM pa.allocation_transfer_id
				AND op.main_account_code IS NOT DISTINCT FROM pa.account_number
				AND op.program_activity_name IS NOT DISTINCT FROM pa.program_activity_name
				AND op.program_activity_code IS NOT DISTINCT FROM pa.program_activity_code
				AND CAST(pa.budget_year as integer) = CAST(sub.reporting_fiscal_year as integer))
	);
