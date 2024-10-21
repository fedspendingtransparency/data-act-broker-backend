-- The combination of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID in File C (award
-- financial) must be unique for USSGL-related balances.
SELECT
    row_number,
    display_tas AS "tas",
    object_class,
    program_activity_code,
    program_activity_name,
    by_direct_reimbursable_fun,
    disaster_emergency_fund_code,
    fain,
    uri,
    piid,
    parent_award_id,
    prior_year_adjustment,
    display_tas AS "uniqueid_TAS",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI",
    piid AS "uniqueid_PIID",
    parent_award_id AS "uniqueid_ParentAwardId",
    prior_year_adjustment AS "uniqueid_PriorYearAdjustment"
FROM (
    SELECT af.row_number,
        af.display_tas,
        af.object_class,
        af.program_activity_code,
        UPPER(af.program_activity_name) AS program_activity_name,
        UPPER(af.by_direct_reimbursable_fun) AS by_direct_reimbursable_fun,
        af.submission_id,
        af.tas,
        af.prior_year_adjustment,
        UPPER(af.fain) AS fain,
        UPPER(af.uri) AS uri,
        UPPER(af.piid) AS piid,
        UPPER(af.parent_award_id) AS parent_award_id,
        UPPER(af.disaster_emergency_fund_code) AS disaster_emergency_fund_code,
        -- numbers all instances of this unique combination incrementally (1, 2, 3, etc)
        ROW_NUMBER() OVER (PARTITION BY
            UPPER(af.display_tas),
            af.object_class,
            COALESCE(af.program_activity_code, ''),
            COALESCE(UPPER(af.program_activity_name), ''),
            UPPER(af.by_direct_reimbursable_fun),
            COALESCE(UPPER(af.prior_year_adjustment), ''),
            COALESCE(UPPER(af.fain), ''),
            COALESCE(UPPER(af.uri), ''),
            COALESCE(UPPER(af.piid), ''),
            COALESCE(UPPER(af.parent_award_id), ''),
            UPPER(af.disaster_emergency_fund_code)
            ORDER BY af.row_number
        ) AS row
    FROM award_financial AS af
    WHERE af.submission_id = {0}
        AND af.transaction_obligated_amou IS NULL
        AND (COALESCE(program_activity_code, '') <> ''
            OR COALESCE(program_activity_name, '') <> '')
    ) duplicates
-- if there is any row numbered over 1, that means there's more than one instance of that unique combination
WHERE duplicates.row > 1;
