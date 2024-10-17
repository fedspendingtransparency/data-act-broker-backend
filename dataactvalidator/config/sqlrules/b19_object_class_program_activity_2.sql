-- The combination of TAS, object class, PARK, reimbursable flag, DEFC, and PYA in File B (object class program
-- activity) must be unique. Object classes in ### and ###0 formats are treated as equivalent for purposes of the
-- uniqueness check.
SELECT
    row_number,
    display_tas AS "tas",
    object_class,
    pa_reporting_key,
    by_direct_reimbursable_fun,
    disaster_emergency_fund_code,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS",
    pa_reporting_key AS "uniqueid_ProgramActivityReportingKey",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment"
FROM (
    SELECT op.row_number,
        op.object_class,
        UPPER(op.pa_reporting_key) AS pa_reporting_key,
        UPPER(op.by_direct_reimbursable_fun) AS by_direct_reimbursable_fun,
        op.submission_id,
        op.tas,
        op.display_tas,
        UPPER(op.disaster_emergency_fund_code) AS disaster_emergency_fund_code,
        prior_year_adjustment,
        -- numbers all instances of this unique combination incrementally (1, 2, 3, etc)
        ROW_NUMBER() OVER (PARTITION BY
            UPPER(display_tas),
            RPAD(op.object_class, 4 ,'0'),
            UPPER(op.pa_reporting_key),
            UPPER(op.by_direct_reimbursable_fun),
            UPPER(op.disaster_emergency_fund_code),
            UPPER(prior_year_adjustment)
            ORDER BY op.row_number
        ) AS row
    FROM object_class_program_activity AS op
    WHERE op.submission_id = {0}
        AND COALESCE(op.pa_reporting_key, '') <> ''
    ) duplicates
-- if there is any row numbered over 1, that means there's more than one instance of that unique combination
WHERE duplicates.row > 1;
