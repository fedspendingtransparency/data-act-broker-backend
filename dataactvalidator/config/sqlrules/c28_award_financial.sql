-- The combination of TAS/object class/program activity code+name/reimbursable flag/DEFC/Award ID in
-- File C (award financial) should be unique for USSGL-related balances.
SELECT
    row_number,
    beginning_period_of_availa,
    ending_period_of_availabil,
    agency_identifier,
    allocation_transfer_agency,
    availability_type_code,
    main_account_code,
    sub_account_code,
    object_class,
    program_activity_code,
    by_direct_reimbursable_fun,
    disaster_emergency_fund_code,
    fain,
    uri,
    piid,
    parent_award_id,
    display_tas AS "uniqueid_TAS",
    program_activity_code AS "uniqueid_ProgramActivityCode",
    program_activity_name AS "uniqueid_ProgramActivityName",
    object_class AS "uniqueid_ObjectClass",
    by_direct_reimbursable_fun AS "uniqueid_ByDirectReimbursableFundingSource",
    disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI",
    piid AS "uniqueid_PIID",
    parent_award_id AS "uniqueid_ParentAwardId"
FROM (
    SELECT af.row_number,
        af.beginning_period_of_availa,
        af.ending_period_of_availabil,
        af.agency_identifier,
        af.allocation_transfer_agency,
        UPPER(af.availability_type_code) AS availability_type_code,
        af.main_account_code,
        af.sub_account_code,
        af.object_class,
        af.program_activity_code,
        UPPER(af.program_activity_name) AS program_activity_name,
        UPPER(af.by_direct_reimbursable_fun) AS by_direct_reimbursable_fun,
        af.submission_id,
        af.tas,
        af.display_tas,
        UPPER(af.fain) AS fain,
        UPPER(af.uri) AS uri,
        UPPER(af.piid) AS piid,
        UPPER(af.parent_award_id) AS parent_award_id,
        UPPER(af.disaster_emergency_fund_code) AS disaster_emergency_fund_code,
        -- numbers all instances of this unique combination incrementally (1, 2, 3, etc)
        ROW_NUMBER() OVER (PARTITION BY
            af.beginning_period_of_availa,
            af.ending_period_of_availabil,
            af.agency_identifier,
            af.allocation_transfer_agency,
            UPPER(af.availability_type_code),
            af.main_account_code,
            af.sub_account_code,
            af.object_class,
            af.program_activity_code,
            UPPER(af.program_activity_name),
            UPPER(af.by_direct_reimbursable_fun),
            UPPER(af.fain),
            UPPER(af.uri),
            UPPER(af.piid),
            UPPER(af.parent_award_id),
            UPPER(af.disaster_emergency_fund_code)
            ORDER BY af.row_number
        ) AS row
    FROM award_financial AS af
    WHERE af.submission_id = {0}
        AND af.transaction_obligated_amou IS NULL
    ) duplicates
-- if there is any row numbered over 1, that means there's more than one instance of that unique combination
WHERE duplicates.row > 1;
