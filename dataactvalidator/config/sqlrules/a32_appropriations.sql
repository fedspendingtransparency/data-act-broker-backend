-- TAS values in File A (appropriation) should be unique
SELECT approp.row_number,
    approp.allocation_transfer_agency,
    approp.agency_identifier,
    approp.beginning_period_of_availa,
    approp.ending_period_of_availabil,
    approp.availability_type_code,
    approp.main_account_code,
    approp.sub_account_code,
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation AS approp
WHERE approp.submission_id = {0}
    AND EXISTS (
        SELECT 1
        FROM appropriation AS other
        WHERE other.row_number <> approp.row_number
            AND other.account_num = approp.account_num
            AND other.submission_id = {0}
    );
