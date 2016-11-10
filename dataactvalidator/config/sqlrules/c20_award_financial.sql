-- This rule selects 32 distinct elements in Files B and C based on TAS/OC combination
-- The elements from both files are summed before comparing
-- As we are comparing sums, we cannot return row numbers, so we select NULL
SELECT NULL AS row_number,
    award_financial_records.tas,
    award_financial_records.object_class,
    ussgl480100_undelivered_or_fyb_sum_c,
    ussgl480100_undelivered_or_cpe_sum_c,
    ussgl483100_undelivered_or_cpe_sum_c,
    ussgl488100_upward_adjustm_cpe_sum_c,
    obligations_undelivered_or_fyb_sum_c,
    obligations_undelivered_or_cpe_sum_c,
    ussgl490100_delivered_orde_fyb_sum_c,
    ussgl490100_delivered_orde_cpe_sum_c,
    ussgl493100_delivered_orde_cpe_sum_c,
    ussgl498100_upward_adjustm_cpe_sum_c,
    obligations_delivered_orde_fyb_sum_c,
    obligations_delivered_orde_cpe_sum_c,
    ussgl480200_undelivered_or_fyb_sum_c,
    ussgl480200_undelivered_or_cpe_sum_c,
    ussgl483200_undelivered_or_cpe_sum_c,
    ussgl488200_upward_adjustm_cpe_sum_c,
    gross_outlays_undelivered_fyb_sum_c,
    gross_outlays_undelivered_cpe_sum_c,
    ussgl490200_delivered_orde_cpe_sum_c,
    ussgl490800_authority_outl_fyb_sum_c,
    ussgl490800_authority_outl_cpe_sum_c,
    ussgl498200_upward_adjustm_cpe_sum_c,
    gross_outlays_delivered_or_fyb_sum_c,
    gross_outlays_delivered_or_cpe_sum_c,
    gross_outlay_amount_by_awa_fyb_sum_c,
    gross_outlay_amount_by_awa_cpe_sum_c,
    obligations_incurred_byawa_cpe_sum_c,
    ussgl487100_downward_adjus_cpe_sum_c,
    ussgl497100_downward_adjus_cpe_sum_c,
    ussgl487200_downward_adjus_cpe_sum_c,
    ussgl497200_downward_adjus_cpe_sum_c,
    deobligations_recov_by_awa_cpe_sum_c,
    ussgl480100_undelivered_or_fyb_sum_b,
    ussgl480100_undelivered_or_cpe_sum_b,
    ussgl483100_undelivered_or_cpe_sum_b,
    ussgl488100_upward_adjustm_cpe_sum_b,
    obligations_undelivered_or_fyb_sum_b,
    obligations_undelivered_or_cpe_sum_b,
    ussgl490100_delivered_orde_fyb_sum_b,
    ussgl490100_delivered_orde_cpe_sum_b,
    ussgl493100_delivered_orde_cpe_sum_b,
    ussgl498100_upward_adjustm_cpe_sum_b,
    obligations_delivered_orde_fyb_sum_b,
    obligations_delivered_orde_cpe_sum_b,
    ussgl480200_undelivered_or_fyb_sum_b,
    ussgl480200_undelivered_or_cpe_sum_b,
    ussgl483200_undelivered_or_cpe_sum_b,
    ussgl488200_upward_adjustm_cpe_sum_b,
    gross_outlays_undelivered_fyb_sum_b,
    gross_outlays_undelivered_cpe_sum_b,
    ussgl490200_delivered_orde_cpe_sum_b,
    ussgl490800_authority_outl_fyb_sum_b,
    ussgl490800_authority_outl_cpe_sum_b,
    ussgl498200_upward_adjustm_cpe_sum_b,
    gross_outlays_delivered_or_fyb_sum_b,
    gross_outlays_delivered_or_cpe_sum_b,
    gross_outlay_amount_by_pro_fyb_sum_b,
    gross_outlay_amount_by_pro_cpe_sum_b,
    obligations_incurred_by_pr_cpe_sum_b,
    ussgl487100_downward_adjus_cpe_sum_b,
    ussgl497100_downward_adjus_cpe_sum_b,
    ussgl487200_downward_adjus_cpe_sum_b,
    ussgl497200_downward_adjus_cpe_sum_b,
    deobligations_recov_by_pro_cpe_sum_b
-- This first subquery is selecting the sum of 32 elements in File C based on TAS, OC, and Submission ID
FROM (
    SELECT SUM(af.ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb_sum_c,
        SUM(af.ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe_sum_c,
        SUM(af.ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe_sum_c,
        SUM(af.ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe_sum_c,
        SUM(af.obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb_sum_c,
        SUM(af.obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe_sum_c,
        SUM(af.ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb_sum_c,
        SUM(af.ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe_sum_c,
        SUM(af.ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe_sum_c,
        SUM(af.ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe_sum_c,
        SUM(af.obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb_sum_c,
        SUM(af.obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe_sum_c,
        SUM(af.ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb_sum_c,
        SUM(af.ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe_sum_c,
        SUM(af.ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe_sum_c,
        SUM(af.ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe_sum_c,
        SUM(af.gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb_sum_c,
        SUM(af.gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe_sum_c,
        SUM(af.ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe_sum_c,
        SUM(af.ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb_sum_c,
        SUM(af.ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe_sum_c,
        SUM(af.ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe_sum_c,
        SUM(af.gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb_sum_c,
        SUM(af.gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe_sum_c,
        SUM(af.gross_outlay_amount_by_awa_fyb) AS gross_outlay_amount_by_awa_fyb_sum_c,
        SUM(af.gross_outlay_amount_by_awa_cpe) AS gross_outlay_amount_by_awa_cpe_sum_c,
        SUM(af.obligations_incurred_byawa_cpe) AS obligations_incurred_byawa_cpe_sum_c,
        SUM(af.ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe_sum_c,
        SUM(af.ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe_sum_c,
        SUM(af.ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe_sum_c,
        SUM(af.ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe_sum_c,
        SUM(af.deobligations_recov_by_awa_cpe) AS deobligations_recov_by_awa_cpe_sum_c,
        af.tas,
        af.object_class
    FROM award_financial AS af
    WHERE af.submission_id = {0}
    GROUP BY af.tas,
        af.object_class,
        af.submission_id
) AS award_financial_records
-- The second subquery selects the sum of the corresponding 32 elements in File B
-- Again, the sum is based on TAS, OC, and Submission ID
-- We do a FULL OUTER JOIN of this result, as we don't care if TAS/OC combindations from File B aren't in File C
FULL OUTER JOIN
(
    SELECT SUM(op.ussgl480100_undelivered_or_fyb) AS ussgl480100_undelivered_or_fyb_sum_b,
        SUM(op.ussgl480100_undelivered_or_cpe) AS ussgl480100_undelivered_or_cpe_sum_b,
        SUM(op.ussgl483100_undelivered_or_cpe) AS ussgl483100_undelivered_or_cpe_sum_b,
        SUM(op.ussgl488100_upward_adjustm_cpe) AS ussgl488100_upward_adjustm_cpe_sum_b,
        SUM(op.obligations_undelivered_or_fyb) AS obligations_undelivered_or_fyb_sum_b,
        SUM(op.obligations_undelivered_or_cpe) AS obligations_undelivered_or_cpe_sum_b,
        SUM(op.ussgl490100_delivered_orde_fyb) AS ussgl490100_delivered_orde_fyb_sum_b,
        SUM(op.ussgl490100_delivered_orde_cpe) AS ussgl490100_delivered_orde_cpe_sum_b,
        SUM(op.ussgl493100_delivered_orde_cpe) AS ussgl493100_delivered_orde_cpe_sum_b,
        SUM(op.ussgl498100_upward_adjustm_cpe) AS ussgl498100_upward_adjustm_cpe_sum_b,
        SUM(op.obligations_delivered_orde_fyb) AS obligations_delivered_orde_fyb_sum_b,
        SUM(op.obligations_delivered_orde_cpe) AS obligations_delivered_orde_cpe_sum_b,
        SUM(op.ussgl480200_undelivered_or_fyb) AS ussgl480200_undelivered_or_fyb_sum_b,
        SUM(op.ussgl480200_undelivered_or_cpe) AS ussgl480200_undelivered_or_cpe_sum_b,
        SUM(op.ussgl483200_undelivered_or_cpe) AS ussgl483200_undelivered_or_cpe_sum_b,
        SUM(op.ussgl488200_upward_adjustm_cpe) AS ussgl488200_upward_adjustm_cpe_sum_b,
        SUM(op.gross_outlays_undelivered_fyb) AS gross_outlays_undelivered_fyb_sum_b,
        SUM(op.gross_outlays_undelivered_cpe) AS gross_outlays_undelivered_cpe_sum_b,
        SUM(op.ussgl490200_delivered_orde_cpe) AS ussgl490200_delivered_orde_cpe_sum_b,
        SUM(op.ussgl490800_authority_outl_fyb) AS ussgl490800_authority_outl_fyb_sum_b,
        SUM(op.ussgl490800_authority_outl_cpe) AS ussgl490800_authority_outl_cpe_sum_b,
        SUM(op.ussgl498200_upward_adjustm_cpe) AS ussgl498200_upward_adjustm_cpe_sum_b,
        SUM(op.gross_outlays_delivered_or_fyb) AS gross_outlays_delivered_or_fyb_sum_b,
        SUM(op.gross_outlays_delivered_or_cpe) AS gross_outlays_delivered_or_cpe_sum_b,
        SUM(op.gross_outlay_amount_by_pro_fyb) AS gross_outlay_amount_by_pro_fyb_sum_b,
        SUM(op.gross_outlay_amount_by_pro_cpe) AS gross_outlay_amount_by_pro_cpe_sum_b,
        SUM(op.obligations_incurred_by_pr_cpe) AS obligations_incurred_by_pr_cpe_sum_b,
        SUM(op.ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe_sum_b,
        SUM(op.ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe_sum_b,
        SUM(op.ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe_sum_b,
        SUM(op.ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe_sum_b,
        SUM(op.deobligations_recov_by_pro_cpe) AS deobligations_recov_by_pro_cpe_sum_b,
        op.tas,
        op.object_class
    FROM object_class_program_activity AS op
    WHERE op.submission_id = {0}
    GROUP BY op.tas,
        op.object_class,
        op.submission_id
-- We join these two subqueries based on the same TAS and OC combination
) AS object_class_records
    ON object_class_records.tas = award_financial_records.tas
        AND object_class_records.object_class = award_financial_records.object_class
-- Negative values are expected, which prompts the use of ABS
-- The total from File B should always be absolutely greater than or equal to the total in File C
-- Thus, we select combindations where the File C sum is greater than the File B sum
WHERE (ABS(ussgl480100_undelivered_or_fyb_sum_c) > ABS(ussgl480100_undelivered_or_fyb_sum_b)
    OR ABS(ussgl480100_undelivered_or_cpe_sum_c) > ABS(ussgl480100_undelivered_or_cpe_sum_b)
    OR ABS(ussgl483100_undelivered_or_cpe_sum_c) > ABS(ussgl483100_undelivered_or_cpe_sum_b)
    OR ABS(ussgl488100_upward_adjustm_cpe_sum_c) > ABS(ussgl488100_upward_adjustm_cpe_sum_b)
    OR ABS(obligations_undelivered_or_fyb_sum_c) > ABS(obligations_undelivered_or_fyb_sum_b)
    OR ABS(obligations_undelivered_or_cpe_sum_c) > ABS(obligations_undelivered_or_cpe_sum_b)
    OR ABS(ussgl490100_delivered_orde_fyb_sum_c) > ABS(ussgl490100_delivered_orde_fyb_sum_b)
    OR ABS(ussgl490100_delivered_orde_cpe_sum_c) > ABS(ussgl490100_delivered_orde_cpe_sum_b)
    OR ABS(ussgl493100_delivered_orde_cpe_sum_c) > ABS(ussgl493100_delivered_orde_cpe_sum_b)
    OR ABS(ussgl498100_upward_adjustm_cpe_sum_c) > ABS(ussgl498100_upward_adjustm_cpe_sum_b)
    OR ABS(obligations_delivered_orde_fyb_sum_c) > ABS(obligations_delivered_orde_fyb_sum_b)
    OR ABS(obligations_delivered_orde_cpe_sum_c) > ABS(obligations_delivered_orde_cpe_sum_b)
    OR ABS(ussgl480200_undelivered_or_fyb_sum_c) > ABS(ussgl480200_undelivered_or_fyb_sum_b)
    OR ABS(ussgl480200_undelivered_or_cpe_sum_c) > ABS(ussgl480200_undelivered_or_cpe_sum_b)
    OR ABS(ussgl483200_undelivered_or_cpe_sum_c) > ABS(ussgl483200_undelivered_or_cpe_sum_b)
    OR ABS(ussgl488200_upward_adjustm_cpe_sum_c) > ABS(ussgl488200_upward_adjustm_cpe_sum_b)
    OR ABS(gross_outlays_undelivered_fyb_sum_c) > ABS(gross_outlays_undelivered_fyb_sum_b)
    OR ABS(gross_outlays_undelivered_cpe_sum_c) > ABS(gross_outlays_undelivered_cpe_sum_b)
    OR ABS(ussgl490200_delivered_orde_cpe_sum_c) > ABS(ussgl490200_delivered_orde_cpe_sum_b)
    OR ABS(ussgl490800_authority_outl_fyb_sum_c) > ABS(ussgl490800_authority_outl_fyb_sum_b)
    OR ABS(ussgl490800_authority_outl_cpe_sum_c) > ABS(ussgl490800_authority_outl_cpe_sum_b)
    OR ABS(ussgl498200_upward_adjustm_cpe_sum_c) > ABS(ussgl498200_upward_adjustm_cpe_sum_b)
    OR ABS(gross_outlays_delivered_or_fyb_sum_c) > ABS(gross_outlays_delivered_or_fyb_sum_b)
    OR ABS(gross_outlays_delivered_or_cpe_sum_c) > ABS(gross_outlays_delivered_or_cpe_sum_b)
    OR ABS(gross_outlay_amount_by_awa_fyb_sum_c) > ABS(gross_outlay_amount_by_pro_fyb_sum_b)
    OR ABS(gross_outlay_amount_by_awa_cpe_sum_c) > ABS(gross_outlay_amount_by_pro_cpe_sum_b)
    OR ABS(obligations_incurred_byawa_cpe_sum_c) > ABS(obligations_incurred_by_pr_cpe_sum_b)
    OR ABS(ussgl487100_downward_adjus_cpe_sum_c) > ABS(ussgl487100_downward_adjus_cpe_sum_b)
    OR ABS(ussgl497100_downward_adjus_cpe_sum_c) > ABS(ussgl497100_downward_adjus_cpe_sum_b)
    OR ABS(ussgl487200_downward_adjus_cpe_sum_c) > ABS(ussgl487200_downward_adjus_cpe_sum_b)
    OR ABS(ussgl497200_downward_adjus_cpe_sum_c) > ABS(ussgl497200_downward_adjus_cpe_sum_b)
    OR ABS(deobligations_recov_by_awa_cpe_sum_c) > ABS(deobligations_recov_by_pro_cpe_sum_b));