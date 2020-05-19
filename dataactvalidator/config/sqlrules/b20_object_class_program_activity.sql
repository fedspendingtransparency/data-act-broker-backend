-- All combinations of TAS/program activity code+name (when provided in File C)/object class in File C (award financial)
-- should exist in File B (object class program activity). Since not all object classes will have award activity, it is
-- acceptable for combinations of TAS/program activity code+name/object class in File C to be a subset of those provided
-- in File B.

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
        program_activity_name,
        object_class,
        tas_id,
        display_tas
    FROM award_financial
    WHERE submission_id = {0}),
ocpa_b20_{0} AS
    (SELECT tas_id,
        program_activity_code,
        program_activity_name,
        object_class
    FROM object_class_program_activity
    WHERE submission_id = {0})
SELECT
    af.row_number AS "source_row_number",
    af.program_activity_code AS "source_value_program_activity_code",
    af.program_activity_name AS "source_value_program_activity_name",
    af.object_class AS "source_value_object_class",
    af.display_tas AS "uniqueid_TAS",
    af.program_activity_code AS "uniqueid_ProgramActivityCode",
    af.program_activity_name AS "uniqueid_ProgramActivityName",
    af.object_class AS "uniqueid_ObjectClass"
FROM award_financial_b20_{0} AS af
WHERE NOT EXISTS (
        SELECT 1
        FROM ocpa_b20_{0} AS op
        WHERE COALESCE(af.tas_id, 0) = COALESCE(op.tas_id, 0)
            AND (COALESCE(af.program_activity_code, '') = COALESCE(op.program_activity_code, '')
                OR COALESCE(af.program_activity_code, '') = ''
                OR af.program_activity_code = '0000'
            )
            AND (UPPER(COALESCE(af.program_activity_name, '')) = UPPER(COALESCE(op.program_activity_name, ''))
                OR COALESCE(af.program_activity_name, '') = ''
            )
            AND (COALESCE(af.object_class, '') = COALESCE(op.object_class, '')
                OR (af.object_class IN ('0', '00', '000', '0000')
                    AND op.object_class IN ('0', '00', '000', '0000')
                )
            )
    );
