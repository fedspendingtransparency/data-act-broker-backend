-- The combination of TAS, object class, program activity code+name, reimbursable flag, DEFC, and PYA in File B (object
-- class program activity) must be unique. Object classes in ### and ###0 formats are treated as equivalent for purposes
-- of the uniqueness check.
SELECT
    row_number,
    display_tas AS "tas",
    object_class,
    program_activity_code,
    program_activity_name,
    by_direct_reimbursable_fun,
    disaster_emergency_fund_code,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment"
FROM (
    SELECT op.row_number,
        op.object_class,
        op.program_activity_code,
        UPPER(op.program_activity_name) AS program_activity_name,
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
            COALESCE(op.program_activity_code, ''),
            COALESCE(UPPER(op.program_activity_name), ''),
            COALESCE(UPPER(op.by_direct_reimbursable_fun), ''),
            UPPER(op.disaster_emergency_fund_code),
            UPPER(prior_year_adjustment)
            ORDER BY op.row_number
        ) AS row
    FROM object_class_program_activity AS op
    WHERE op.submission_id = {0}
        AND (COALESCE(op.program_activity_code, '') <> ''
            OR COALESCE(op.program_activity_name, '') <> ''
        )
    ) duplicates
-- if there is any row numbered over 1, that means there's more than one instance of that unique combination
WHERE duplicates.row > 1;