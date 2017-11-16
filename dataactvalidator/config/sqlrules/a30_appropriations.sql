-- All TAS values in File A (appropriation) should exist in File B (object class program activity)
SELECT approp.row_number,
    approp.allocation_transfer_agency,
    approp.agency_identifier,
    approp.beginning_period_of_availa,
    approp.ending_period_of_availabil,
    approp.availability_type_code,
    approp.main_account_code,
    approp.sub_account_code
FROM appropriation AS approp
WHERE approp.submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM object_class_program_activity AS op
        WHERE approp.tas_id IS NOT DISTINCT FROM op.tas_id
            AND approp.submission_id = op.submission_id
    );
