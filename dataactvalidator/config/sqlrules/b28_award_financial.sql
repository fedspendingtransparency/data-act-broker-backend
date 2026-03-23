-- Should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined in the OMB’s
-- Program Activity Mapping File. Ignore rule for $0 rows
SELECT
    row_number,
    program_activity_reporting_key,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM award_financial AS af
WHERE submission_id = {0}
    AND COALESCE(program_activity_reporting_key, '') <> ''
    -- Checking for PARKs in the database without sub accounts listed
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity_park AS park
        WHERE UPPER(program_activity_reporting_key) = UPPER(park_code)
            AND park.sub_account_number IS NULL
            AND COALESCE(park.agency_id, '') = COALESCE(af.agency_identifier, '')
            AND COALESCE(park.allocation_transfer_id, '') = COALESCE(af.allocation_transfer_agency, '')
            AND COALESCE(park.main_account_number, '') = COALESCE(af.main_account_code, '')
    )
    -- Checking for PARKs in the database with sub accounts listed
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity_park AS park
        WHERE UPPER(program_activity_reporting_key) = UPPER(park_code)
            AND park.sub_account_number IS NOT NULL
            AND COALESCE(park.agency_id, '') = COALESCE(af.agency_identifier, '')
            AND COALESCE(park.allocation_transfer_id, '') = COALESCE(af.allocation_transfer_agency, '')
            AND COALESCE(park.main_account_number, '') = COALESCE(af.main_account_code, '')
            AND COALESCE(park.sub_account_number, '') = COALESCE(af.sub_account_code, '')
    );