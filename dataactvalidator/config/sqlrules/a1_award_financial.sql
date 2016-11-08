SELECT
    row_number,
    allocation_transfer_agency,
    agency_identifier,
    beginning_period_of_availa,
    ending_period_of_availabil,
    availability_type_code,
    main_account_code,
    sub_account_code
FROM award_financial as af
LEFT JOIN submission ON (af.submission_id = submission.submission_id)
WHERE af.submission_id = {}
AND NOT EXISTS(
    SELECT 1 FROM tas_lookup as tas
    WHERE af.allocation_transfer_agency IS NOT DISTINCT FROM tas.allocation_transfer_agency
    AND af.agency_identifier IS NOT DISTINCT FROM tas.agency_identifier
    AND af.beginning_period_of_availa IS NOT DISTINCT FROM tas.beginning_period_of_availability
    AND af.ending_period_of_availabil IS NOT DISTINCT FROM tas.ending_period_of_availability
    AND af.availability_type_code IS NOT DISTINCT FROM tas.availability_type_code
    AND af.main_account_code IS NOT DISTINCT FROM tas.main_account_code
    AND af.sub_account_code IS NOT DISTINCT FROM tas.sub_account_code
    AND (submission.reporting_start_date, submission.reporting_end_date)
        -- A null end date indicates "still open". To make OVERLAPS work,
        -- we'll use the end date of the submission to achieve the same result
        OVERLAPS (tas.internal_start_date, COALESCE(tas.internal_end_date, submission.reporting_end_date))
)
