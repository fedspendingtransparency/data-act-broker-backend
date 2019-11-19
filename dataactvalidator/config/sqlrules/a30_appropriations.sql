-- All TAS values in File A (appropriation) should exist in File B (object class program activity)
SELECT approp.row_number AS "source_row_number",
    approp.allocation_transfer_agency AS "source_value_allocation_transfer_agency",
    approp.agency_identifier AS "source_value_agency_identifier",
    approp.beginning_period_of_availa AS "source_value_beginning_period_of_availa",
    approp.ending_period_of_availabil AS "source_value_ending_period_of_availabil",
    approp.availability_type_code AS "source_value_availability_type_code",
    approp.main_account_code AS "source_value_main_account_code",
    approp.sub_account_code AS "source_value_sub_account_code",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation AS approp
WHERE approp.submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM object_class_program_activity AS op
        WHERE approp.tas_id IS NOT DISTINCT FROM op.tas_id
            AND approp.submission_id = op.submission_id
    );
