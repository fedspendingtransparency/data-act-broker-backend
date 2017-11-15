-- All combinations of TAS/program activity code/object class in File C (award financial) should exist in File B
-- (object class program activity).
WITH award_financial_b20_{0} AS
	(SELECT row_number,
		allocation_transfer_agency,
		agency_identifier,
		beginning_period_of_availa,
		ending_period_of_availabil,
		availability_type_code,
		main_account_code,
		sub_account_code,
		program_activity_code,
		object_class,
		tas_id
	FROM award_financial
	WHERE submission_id = {0}),
ocpa_b20_{0} AS
    (SELECT tas_id,
        program_activity_code,
        object_class
	FROM object_class_program_activity
	WHERE submission_id = {0})
SELECT
    af.row_number,
	af.allocation_transfer_agency,
	af.agency_identifier,
	af.beginning_period_of_availa,
	af.ending_period_of_availabil,
	af.availability_type_code,
	af.main_account_code,
	af.sub_account_code,
	af.program_activity_code,
	af.object_class
FROM award_financial_b20_{0} AS af
WHERE NOT EXISTS (
        SELECT 1
        FROM ocpa_b20_{0} AS op
        WHERE COALESCE(af.tas_id, 0) = COALESCE(op.tas_id, 0)
            AND (COALESCE(af.program_activity_code, '') = COALESCE(op.program_activity_code, '')
                OR COALESCE(af.program_activity_code, '') = ''
                OR af.program_activity_code = '0000'
            )
            AND (COALESCE(af.object_class, '') = COALESCE(op.object_class, '')
                OR (af.object_class IN ('0', '00', '000', '0000')
                    AND af.object_class IN ('0', '00', '000', '0000')
                )
            )
    );
