-- Should be a valid ProgramActivityReportingKey (PARK) for the corresponding funding TAS/TAFS, as defined in the OMB’s
-- Program Activity Mapping File. Ignore rule for $0 rows
SELECT
    row_number,
    program_activity_reporting_key,
    by_direct_reimbursable_fun,
    display_tas AS "uniqueid_TAS",
    object_class AS "uniqueid_ObjectClass"
FROM object_class_program_activity AS ocpa
WHERE submission_id = {0}
    AND COALESCE(program_activity_reporting_key, '') <> ''
    AND (COALESCE(ussgl480100_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480100_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl480110_rein_undel_ord_cpe, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl480200_undelivered_or_fyb, 0) <> 0
        OR COALESCE(ussgl480210_rein_undel_obs_cpe, 0) <> 0
        OR COALESCE(ussgl483100_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl483200_undelivered_or_cpe, 0) <> 0
        OR COALESCE(ussgl487100_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl487200_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl488100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl488200_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_fyb, 0) <> 0
        OR COALESCE(ussgl490100_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl490110_rein_deliv_ord_cpe, 0) <> 0
        OR COALESCE(ussgl490200_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_fyb, 0) <> 0
        OR COALESCE(ussgl490800_authority_outl_cpe, 0) <> 0
        OR COALESCE(ussgl493100_delivered_orde_cpe, 0) <> 0
        OR COALESCE(ussgl497100_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl497200_downward_adjus_cpe, 0) <> 0
        OR COALESCE(ussgl497210_down_adj_refun_cpe, 0) <> 0
        OR COALESCE(ussgl498100_upward_adjustm_cpe, 0) <> 0
        OR COALESCE(ussgl498200_upward_adjustm_cpe, 0) <> 0
    )
    -- Checking for PARKs in the database without sub accounts listed
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity_park AS park
        WHERE UPPER(program_activity_reporting_key) = UPPER(park_code)
            AND park.sub_account_number IS NULL
            AND COALESCE(park.agency_id, '') = COALESCE(ocpa.agency_identifier, '')
            AND COALESCE(park.allocation_transfer_id, '') = COALESCE(ocpa.allocation_transfer_agency, '')
            AND COALESCE(park.main_account_number, '') = COALESCE(ocpa.main_account_code, '')
    )
    -- Checking for PARKs in the database with sub accounts listed
    AND NOT EXISTS (
        SELECT 1
        FROM program_activity_park AS park
        WHERE UPPER(program_activity_reporting_key) = UPPER(park_code)
            AND park.sub_account_number IS NOT NULL
            AND COALESCE(park.agency_id, '') = COALESCE(ocpa.agency_identifier, '')
            AND COALESCE(park.allocation_transfer_id, '') = COALESCE(ocpa.allocation_transfer_agency, '')
            AND COALESCE(park.main_account_number, '') = COALESCE(ocpa.main_account_code, '')
            AND COALESCE(park.sub_account_number, '') = COALESCE(ocpa.sub_account_code, '')
    );