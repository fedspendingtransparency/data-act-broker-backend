SELECT
    row_number,
    allocation_transfer_agency,
    agency_identifier,
    beginning_period_of_availa,
    ending_period_of_availabil,
    availability_type_code,
    main_account_code,
    sub_account_code
FROM appropriation as approp
LEFT JOIN submission ON (approp.submission_id = submission.submission_id)
WHERE approp.submission_id = {}
AND NOT EXISTS(
    SELECT 1 FROM tas_lookup as tas
    WHERE approp.allocation_transfer_agency IS NOT DISTINCT FROM tas.allocation_transfer_agency
    AND approp.agency_identifier IS NOT DISTINCT FROM tas.agency_identifier
    AND approp.beginning_period_of_availa IS NOT DISTINCT FROM tas.beginning_period_of_availability
    AND approp.ending_period_of_availabil IS NOT DISTINCT FROM tas.ending_period_of_availability
    AND approp.availability_type_code IS NOT DISTINCT FROM tas.availability_type_code
    AND approp.main_account_code IS NOT DISTINCT FROM tas.main_account_code
    AND approp.sub_account_code IS NOT DISTINCT FROM tas.sub_account_code
    AND (submission.reporting_start_date, submission.reporting_end_date)
        -- A null end date indicates "still open". To make OVERLAPS work,
        -- we'll use the end date of the submission to achieve the same result
        OVERLAPS (tas.internal_start_date, COALESCE(tas.internal_end_date, submission.reporting_end_date))
)
