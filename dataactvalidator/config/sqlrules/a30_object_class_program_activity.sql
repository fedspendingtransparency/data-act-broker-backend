-- All TAS values in File B (object class program activity) should exist in File A (appropriation)
SELECT op.row_number AS "source_row_number",
    op.allocation_transfer_agency AS "source_value_allocation_transfer_agency",
    op.agency_identifier AS "source_value_agency_identifier",
    op.beginning_period_of_availa AS "source_value_beginning_period_of_availa",
    op.ending_period_of_availabil AS "source_value_ending_period_of_availabil",
    op.availability_type_code AS "source_value_availability_type_code",
    op.main_account_code AS "source_value_main_account_code",
    op.sub_account_code AS "source_value_sub_account_code",
    op.tas AS "uniqueid_TAS"
FROM object_class_program_activity AS op
WHERE op.submission_id = {0}
    AND NOT EXISTS (
        SELECT 1
        FROM appropriation AS approp
        WHERE op.tas_id IS NOT DISTINCT FROM approp.tas_id
            AND op.submission_id = approp.submission_id
    );
