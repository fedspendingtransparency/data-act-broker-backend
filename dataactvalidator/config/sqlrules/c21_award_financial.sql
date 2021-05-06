-- Each USSGL account balance or subtotal, when totaled by combination of TAS/program activity code provided in File C,
-- should be a subset of, or equal to, the same combinations in File B. For example, -10 in C and -100 in B
-- would pass.
-- This rule selects 32 distinct elements in Files B and C based on TAS/PAC combination
-- The elements from both files are summed before comparing
-- As we are comparing sums, we cannot return row numbers, so we select NULL
SELECT NULL AS "source_row_number",
    award_financial_records.display_tas AS "source_value_tas",
    award_financial_records.program_activity_code AS "source_value_program_activity_code",
    ussgl480100_undelivered_or_fyb_sum_c AS "source_value_ussgl480100_undelivered_or_fyb_sum_c",
    ussgl480100_undelivered_or_cpe_sum_c AS "source_value_ussgl480100_undelivered_or_cpe_sum_c",
    ussgl483100_undelivered_or_cpe_sum_c AS "source_value_ussgl483100_undelivered_or_cpe_sum_c",
    ussgl488100_upward_adjustm_cpe_sum_c AS "source_value_ussgl488100_upward_adjustm_cpe_sum_c",
    obligations_undelivered_or_fyb_sum_c AS "source_value_obligations_undelivered_or_fyb_sum_c",
    obligations_undelivered_or_cpe_sum_c AS "source_value_obligations_undelivered_or_cpe_sum_c",
    ussgl490100_delivered_orde_fyb_sum_c AS "source_value_ussgl490100_delivered_orde_fyb_sum_c",
    ussgl490100_delivered_orde_cpe_sum_c AS "source_value_ussgl490100_delivered_orde_cpe_sum_c",
    ussgl493100_delivered_orde_cpe_sum_c AS "source_value_ussgl493100_delivered_orde_cpe_sum_c",
    ussgl498100_upward_adjustm_cpe_sum_c AS "source_value_ussgl498100_upward_adjustm_cpe_sum_c",
    obligations_delivered_orde_fyb_sum_c AS "source_value_obligations_delivered_orde_fyb_sum_c",
    obligations_delivered_orde_cpe_sum_c AS "source_value_obligations_delivered_orde_cpe_sum_c",
    ussgl480200_undelivered_or_fyb_sum_c AS "source_value_ussgl480200_undelivered_or_fyb_sum_c",
    ussgl480200_undelivered_or_cpe_sum_c AS "source_value_ussgl480200_undelivered_or_cpe_sum_c",
    ussgl483200_undelivered_or_cpe_sum_c AS "source_value_ussgl483200_undelivered_or_cpe_sum_c",
    ussgl488200_upward_adjustm_cpe_sum_c AS "source_value_ussgl488200_upward_adjustm_cpe_sum_c",
    gross_outlays_undelivered_fyb_sum_c AS "source_value_gross_outlays_undelivered_fyb_sum_c",
    gross_outlays_undelivered_cpe_sum_c AS "source_value_gross_outlays_undelivered_cpe_sum_c",
    ussgl490200_delivered_orde_cpe_sum_c AS "source_value_ussgl490200_delivered_orde_cpe_sum_c",
    ussgl490800_authority_outl_fyb_sum_c AS "source_value_ussgl490800_authority_outl_fyb_sum_c",
    ussgl490800_authority_outl_cpe_sum_c AS "source_value_ussgl490800_authority_outl_cpe_sum_c",
    ussgl498200_upward_adjustm_cpe_sum_c AS "source_value_ussgl498200_upward_adjustm_cpe_sum_c",
    gross_outlays_delivered_or_fyb_sum_c AS "source_value_gross_outlays_delivered_or_fyb_sum_c",
    gross_outlays_delivered_or_cpe_sum_c AS "source_value_gross_outlays_delivered_or_cpe_sum_c",
    gross_outlay_amount_by_awa_fyb_sum_c AS "source_value_gross_outlay_amount_by_awa_fyb_sum_c",
    gross_outlay_amount_by_awa_cpe_sum_c AS "source_value_gross_outlay_amount_by_awa_cpe_sum_c",
    obligations_incurred_byawa_cpe_sum_c AS "source_value_obligations_incurred_byawa_cpe_sum_c",
    ussgl487100_downward_adjus_cpe_sum_c AS "source_value_ussgl487100_downward_adjus_cpe_sum_c",
    ussgl497100_downward_adjus_cpe_sum_c AS "source_value_ussgl497100_downward_adjus_cpe_sum_c",
    ussgl487200_downward_adjus_cpe_sum_c AS "source_value_ussgl487200_downward_adjus_cpe_sum_c",
    ussgl497200_downward_adjus_cpe_sum_c AS "source_value_ussgl497200_downward_adjus_cpe_sum_c",
    deobligations_recov_by_awa_cpe_sum_c AS "source_value_deobligations_recov_by_awa_cpe_sum_c",
    ussgl480100_undelivered_or_fyb_sum_b AS "target_value_ussgl480100_undelivered_or_fyb_sum_b",
    ussgl480100_undelivered_or_cpe_sum_b AS "target_value_ussgl480100_undelivered_or_cpe_sum_b",
    ussgl483100_undelivered_or_cpe_sum_b AS "target_value_ussgl483100_undelivered_or_cpe_sum_b",
    ussgl488100_upward_adjustm_cpe_sum_b AS "target_value_ussgl488100_upward_adjustm_cpe_sum_b",
    obligations_undelivered_or_fyb_sum_b AS "target_value_obligations_undelivered_or_fyb_sum_b",
    obligations_undelivered_or_cpe_sum_b AS "target_value_obligations_undelivered_or_cpe_sum_b",
    ussgl490100_delivered_orde_fyb_sum_b AS "target_value_ussgl490100_delivered_orde_fyb_sum_b",
    ussgl490100_delivered_orde_cpe_sum_b AS "target_value_ussgl490100_delivered_orde_cpe_sum_b",
    ussgl493100_delivered_orde_cpe_sum_b AS "target_value_ussgl493100_delivered_orde_cpe_sum_b",
    ussgl498100_upward_adjustm_cpe_sum_b AS "target_value_ussgl498100_upward_adjustm_cpe_sum_b",
    obligations_delivered_orde_fyb_sum_b AS "target_value_obligations_delivered_orde_fyb_sum_b",
    obligations_delivered_orde_cpe_sum_b AS "target_value_obligations_delivered_orde_cpe_sum_b",
    ussgl480200_undelivered_or_fyb_sum_b AS "target_value_ussgl480200_undelivered_or_fyb_sum_b",
    ussgl480200_undelivered_or_cpe_sum_b AS "target_value_ussgl480200_undelivered_or_cpe_sum_b",
    ussgl483200_undelivered_or_cpe_sum_b AS "target_value_ussgl483200_undelivered_or_cpe_sum_b",
    ussgl488200_upward_adjustm_cpe_sum_b AS "target_value_ussgl488200_upward_adjustm_cpe_sum_b",
    gross_outlays_undelivered_fyb_sum_b AS "target_value_gross_outlays_undelivered_fyb_sum_b",
    gross_outlays_undelivered_cpe_sum_b AS "target_value_gross_outlays_undelivered_cpe_sum_b",
    ussgl490200_delivered_orde_cpe_sum_b AS "target_value_ussgl490200_delivered_orde_cpe_sum_b",
    ussgl490800_authority_outl_fyb_sum_b AS "target_value_ussgl490800_authority_outl_fyb_sum_b",
    ussgl490800_authority_outl_cpe_sum_b AS "target_value_ussgl490800_authority_outl_cpe_sum_b",
    ussgl498200_upward_adjustm_cpe_sum_b AS "target_value_ussgl498200_upward_adjustm_cpe_sum_b",
    gross_outlays_delivered_or_fyb_sum_b AS "target_value_gross_outlays_delivered_or_fyb_sum_b",
    gross_outlays_delivered_or_cpe_sum_b AS "target_value_gross_outlays_delivered_or_cpe_sum_b",
    gross_outlay_amount_by_pro_fyb_sum_b AS "target_value_gross_outlay_amount_by_pro_fyb_sum_b",
    gross_outlay_amount_by_pro_cpe_sum_b AS "target_value_gross_outlay_amount_by_pro_cpe_sum_b",
    obligations_incurred_by_pr_cpe_sum_b AS "target_value_obligations_incurred_by_pr_cpe_sum_b",
    ussgl487100_downward_adjus_cpe_sum_b AS "target_value_ussgl487100_downward_adjus_cpe_sum_b",
    ussgl497100_downward_adjus_cpe_sum_b AS "target_value_ussgl497100_downward_adjus_cpe_sum_b",
    ussgl487200_downward_adjus_cpe_sum_b AS "target_value_ussgl487200_downward_adjus_cpe_sum_b",
    ussgl497200_downward_adjus_cpe_sum_b AS "target_value_ussgl497200_downward_adjus_cpe_sum_b",
    deobligations_recov_by_pro_cpe_sum_b AS "target_value_deobligations_recov_by_pro_cpe_sum_b",
    CONCAT_WS(', ',
        CASE WHEN ussgl480100_undelivered_or_fyb_sum_c < ussgl480100_undelivered_or_fyb_sum_b
                AND COALESCE(ussgl480100_undelivered_or_fyb_sum_c - ussgl480100_undelivered_or_fyb_sum_b, 0) != 0
            THEN 'ussgl480100_undelivered_or_fyb_sum: ' || (ussgl480100_undelivered_or_fyb_sum_c - ussgl480100_undelivered_or_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl480100_undelivered_or_cpe_sum_c < ussgl480100_undelivered_or_cpe_sum_b
                AND COALESCE(ussgl480100_undelivered_or_cpe_sum_c - ussgl480100_undelivered_or_cpe_sum_b, 0) != 0
            THEN 'ussgl480100_undelivered_or_cpe_sum: ' || (ussgl480100_undelivered_or_cpe_sum_c - ussgl480100_undelivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl483100_undelivered_or_cpe_sum_c < ussgl483100_undelivered_or_cpe_sum_b
                AND COALESCE(ussgl483100_undelivered_or_cpe_sum_c - ussgl483100_undelivered_or_cpe_sum_b, 0) != 0
            THEN 'ussgl483100_undelivered_or_cpe_sum: ' || (ussgl483100_undelivered_or_cpe_sum_c - ussgl483100_undelivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl488100_upward_adjustm_cpe_sum_c < ussgl488100_upward_adjustm_cpe_sum_b
                AND COALESCE(ussgl488100_upward_adjustm_cpe_sum_c - ussgl488100_upward_adjustm_cpe_sum_b, 0) != 0
            THEN 'ussgl488100_upward_adjustm_cpe_sum: ' || (ussgl488100_upward_adjustm_cpe_sum_c - ussgl488100_upward_adjustm_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN obligations_undelivered_or_fyb_sum_c < obligations_undelivered_or_fyb_sum_b
                AND COALESCE(obligations_undelivered_or_fyb_sum_c - obligations_undelivered_or_fyb_sum_b, 0) != 0
            THEN 'obligations_undelivered_or_fyb_sum: ' || (obligations_undelivered_or_fyb_sum_c - obligations_undelivered_or_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN obligations_undelivered_or_cpe_sum_c < obligations_undelivered_or_cpe_sum_b
                AND COALESCE(obligations_undelivered_or_cpe_sum_c - obligations_undelivered_or_cpe_sum_b, 0) != 0
            THEN 'obligations_undelivered_or_cpe_sum: ' || (obligations_undelivered_or_cpe_sum_c - obligations_undelivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl490100_delivered_orde_fyb_sum_c < ussgl490100_delivered_orde_fyb_sum_b
                AND COALESCE(ussgl490100_delivered_orde_fyb_sum_c - ussgl490100_delivered_orde_fyb_sum_b, 0) != 0
            THEN 'ussgl490100_delivered_orde_fyb_sum: ' || (ussgl490100_delivered_orde_fyb_sum_c - ussgl490100_delivered_orde_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl490100_delivered_orde_cpe_sum_c < ussgl490100_delivered_orde_cpe_sum_b
                AND COALESCE(ussgl490100_delivered_orde_cpe_sum_c - ussgl490100_delivered_orde_cpe_sum_b, 0) != 0
            THEN 'ussgl490100_delivered_orde_cpe_sum: ' || (ussgl490100_delivered_orde_cpe_sum_c - ussgl490100_delivered_orde_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl493100_delivered_orde_cpe_sum_c < ussgl493100_delivered_orde_cpe_sum_b
                AND COALESCE(ussgl493100_delivered_orde_cpe_sum_c - ussgl493100_delivered_orde_cpe_sum_b, 0) != 0
            THEN 'ussgl493100_delivered_orde_cpe_sum: ' || (ussgl493100_delivered_orde_cpe_sum_c - ussgl493100_delivered_orde_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl498100_upward_adjustm_cpe_sum_c < ussgl498100_upward_adjustm_cpe_sum_b
                AND COALESCE(ussgl498100_upward_adjustm_cpe_sum_c - ussgl498100_upward_adjustm_cpe_sum_b, 0) != 0
            THEN 'ussgl498100_upward_adjustm_cpe_sum: ' || (ussgl498100_upward_adjustm_cpe_sum_c - ussgl498100_upward_adjustm_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN obligations_delivered_orde_fyb_sum_c < obligations_delivered_orde_fyb_sum_b
                AND COALESCE(obligations_delivered_orde_fyb_sum_c - obligations_delivered_orde_fyb_sum_b, 0) != 0
            THEN 'obligations_delivered_orde_fyb_sum: ' || (obligations_delivered_orde_fyb_sum_c - obligations_delivered_orde_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN obligations_delivered_orde_cpe_sum_c < obligations_delivered_orde_cpe_sum_b
                AND COALESCE(obligations_delivered_orde_cpe_sum_c - obligations_delivered_orde_cpe_sum_b, 0) != 0
            THEN 'obligations_delivered_orde_cpe_sum: ' || (obligations_delivered_orde_cpe_sum_c - obligations_delivered_orde_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl480200_undelivered_or_fyb_sum_c < ussgl480200_undelivered_or_fyb_sum_b
                AND COALESCE(ussgl480200_undelivered_or_fyb_sum_c - ussgl480200_undelivered_or_fyb_sum_b, 0) != 0
            THEN 'ussgl480200_undelivered_or_fyb_sum: ' || (ussgl480200_undelivered_or_fyb_sum_c - ussgl480200_undelivered_or_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl480200_undelivered_or_cpe_sum_c < ussgl480200_undelivered_or_cpe_sum_b
                AND COALESCE(ussgl480200_undelivered_or_cpe_sum_c - ussgl480200_undelivered_or_cpe_sum_b, 0) != 0
            THEN 'ussgl480200_undelivered_or_cpe_sum: ' || (ussgl480200_undelivered_or_cpe_sum_c - ussgl480200_undelivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl483200_undelivered_or_cpe_sum_c < ussgl483200_undelivered_or_cpe_sum_b
                AND COALESCE(ussgl483200_undelivered_or_cpe_sum_c - ussgl483200_undelivered_or_cpe_sum_b, 0) != 0
            THEN 'ussgl483200_undelivered_or_cpe_sum: ' || (ussgl483200_undelivered_or_cpe_sum_c - ussgl483200_undelivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl488200_upward_adjustm_cpe_sum_c < ussgl488200_upward_adjustm_cpe_sum_b
                AND COALESCE(ussgl488200_upward_adjustm_cpe_sum_c - ussgl488200_upward_adjustm_cpe_sum_b, 0) != 0
            THEN 'ussgl488200_upward_adjustm_cpe_sum: ' || (ussgl488200_upward_adjustm_cpe_sum_c - ussgl488200_upward_adjustm_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlays_undelivered_fyb_sum_c < gross_outlays_undelivered_fyb_sum_b
                AND COALESCE(gross_outlays_undelivered_fyb_sum_c - gross_outlays_undelivered_fyb_sum_b, 0) != 0
            THEN 'gross_outlays_undelivered_fyb_sum: ' || (gross_outlays_undelivered_fyb_sum_c - gross_outlays_undelivered_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlays_undelivered_cpe_sum_c < gross_outlays_undelivered_cpe_sum_b
                AND COALESCE(gross_outlays_undelivered_cpe_sum_c - gross_outlays_undelivered_cpe_sum_b, 0) != 0
            THEN 'gross_outlays_undelivered_cpe_sum: ' || (gross_outlays_undelivered_cpe_sum_c - gross_outlays_undelivered_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl490200_delivered_orde_cpe_sum_c < ussgl490200_delivered_orde_cpe_sum_b
                AND COALESCE(ussgl490200_delivered_orde_cpe_sum_c - ussgl490200_delivered_orde_cpe_sum_b, 0) != 0
            THEN 'ussgl490200_delivered_orde_cpe_sum: ' || (ussgl490200_delivered_orde_cpe_sum_c - ussgl490200_delivered_orde_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl490800_authority_outl_fyb_sum_c < ussgl490800_authority_outl_fyb_sum_b
                AND COALESCE(ussgl490800_authority_outl_fyb_sum_c - ussgl490800_authority_outl_fyb_sum_b, 0) != 0
            THEN 'ussgl490800_authority_outl_fyb_sum: ' || (ussgl490800_authority_outl_fyb_sum_c - ussgl490800_authority_outl_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl490800_authority_outl_cpe_sum_c < ussgl490800_authority_outl_cpe_sum_b
                AND COALESCE(ussgl490800_authority_outl_cpe_sum_c - ussgl490800_authority_outl_cpe_sum_b, 0) != 0
            THEN 'ussgl490800_authority_outl_cpe_sum: ' || (ussgl490800_authority_outl_cpe_sum_c - ussgl490800_authority_outl_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl498200_upward_adjustm_cpe_sum_c < ussgl498200_upward_adjustm_cpe_sum_b
                AND COALESCE(ussgl498200_upward_adjustm_cpe_sum_c - ussgl498200_upward_adjustm_cpe_sum_b, 0) != 0
            THEN 'ussgl498200_upward_adjustm_cpe_sum: ' || (ussgl498200_upward_adjustm_cpe_sum_c - ussgl498200_upward_adjustm_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlays_delivered_or_fyb_sum_c < gross_outlays_delivered_or_fyb_sum_b
                AND COALESCE(gross_outlays_delivered_or_fyb_sum_c - gross_outlays_delivered_or_fyb_sum_b, 0) != 0
            THEN 'gross_outlays_delivered_or_fyb_sum: ' || (gross_outlays_delivered_or_fyb_sum_c - gross_outlays_delivered_or_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlays_delivered_or_cpe_sum_c < gross_outlays_delivered_or_cpe_sum_b
                AND COALESCE(gross_outlays_delivered_or_cpe_sum_c - gross_outlays_delivered_or_cpe_sum_b, 0) != 0
            THEN 'gross_outlays_delivered_or_cpe_sum: ' || (gross_outlays_delivered_or_cpe_sum_c - gross_outlays_delivered_or_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlay_amount_by_awa_fyb_sum_c < gross_outlay_amount_by_pro_fyb_sum_b
                AND COALESCE(gross_outlay_amount_by_awa_fyb_sum_c - gross_outlay_amount_by_pro_fyb_sum_b, 0) != 0
            THEN 'gross_outlay_amount_by_awa_fyb_sum: ' || (gross_outlay_amount_by_awa_fyb_sum_c - gross_outlay_amount_by_pro_fyb_sum_b)
            ELSE NULL
            END,
        CASE WHEN gross_outlay_amount_by_awa_cpe_sum_c < gross_outlay_amount_by_pro_cpe_sum_b
                AND COALESCE(gross_outlay_amount_by_awa_cpe_sum_c - gross_outlay_amount_by_pro_cpe_sum_b, 0) != 0
            THEN 'gross_outlay_amount_by_awa_cpe_sum: ' || (gross_outlay_amount_by_awa_cpe_sum_c - gross_outlay_amount_by_pro_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN obligations_incurred_byawa_cpe_sum_c < obligations_incurred_by_pr_cpe_sum_b
                AND COALESCE(obligations_incurred_byawa_cpe_sum_c - obligations_incurred_by_pr_cpe_sum_b, 0) != 0
            THEN 'obligations_incurred_byawa_cpe_sum: ' || (obligations_incurred_byawa_cpe_sum_c - obligations_incurred_by_pr_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl487100_downward_adjus_cpe_sum_c > ussgl487100_downward_adjus_cpe_sum_b
                AND COALESCE(ussgl487100_downward_adjus_cpe_sum_c - ussgl487100_downward_adjus_cpe_sum_b, 0) != 0
            THEN 'ussgl487100_downward_adjus_cpe_sum: ' || (ussgl487100_downward_adjus_cpe_sum_c - ussgl487100_downward_adjus_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl497100_downward_adjus_cpe_sum_c > ussgl497100_downward_adjus_cpe_sum_b
                AND COALESCE(ussgl497100_downward_adjus_cpe_sum_c - ussgl497100_downward_adjus_cpe_sum_b, 0) != 0
            THEN 'ussgl497100_downward_adjus_cpe_sum: ' || (ussgl497100_downward_adjus_cpe_sum_c - ussgl497100_downward_adjus_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl487200_downward_adjus_cpe_sum_c > ussgl487200_downward_adjus_cpe_sum_b
                AND COALESCE(ussgl487200_downward_adjus_cpe_sum_c - ussgl487200_downward_adjus_cpe_sum_b, 0) != 0
            THEN 'ussgl487200_downward_adjus_cpe_sum: ' || (ussgl487200_downward_adjus_cpe_sum_c - ussgl487200_downward_adjus_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN ussgl497200_downward_adjus_cpe_sum_c > ussgl497200_downward_adjus_cpe_sum_b
                AND COALESCE(ussgl497200_downward_adjus_cpe_sum_c - ussgl497200_downward_adjus_cpe_sum_b, 0) != 0
            THEN 'ussgl497200_downward_adjus_cpe_sum: ' || (ussgl497200_downward_adjus_cpe_sum_c - ussgl497200_downward_adjus_cpe_sum_b)
            ELSE NULL
            END,
        CASE WHEN deobligations_recov_by_awa_cpe_sum_c > deobligations_recov_by_pro_cpe_sum_b
                AND COALESCE(deobligations_recov_by_awa_cpe_sum_c - deobligations_recov_by_pro_cpe_sum_b, 0) != 0
            THEN 'deobligations_recov_by_awa_cpe_sum: ' || (deobligations_recov_by_awa_cpe_sum_c - deobligations_recov_by_pro_cpe_sum_b)
            ELSE NULL
            END) AS "difference",
    award_financial_records.display_tas AS "uniqueid_TAS",
    award_financial_records.program_activity_code AS "uniqueid_ProgramActivityCode"
-- This first subquery is selecting the sum of 32 elements in File C based on TAS, PAC, and Submission ID
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
        af.program_activity_code,
        af.display_tas
    FROM award_financial AS af
    WHERE af.submission_id = {0}
    GROUP BY af.tas,
        af.program_activity_code,
        af.display_tas,
        af.submission_id
) AS award_financial_records
-- The second subquery selects the sum of the corresponding 32 elements in File B
-- Again, the sum is based on TAS, PAC, and Submission ID
-- We do a FULL OUTER JOIN of this result, as we don't care if TAS/PAC combinations from File B aren't in File C
FULL OUTER JOIN (
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
        op.program_activity_code
    FROM object_class_program_activity AS op
    WHERE op.submission_id = {0}
    GROUP BY op.tas,
        op.program_activity_code,
        op.submission_id
) AS object_class_records
    -- We join these two subqueries based on the same TAS and PAC combination
    ON object_class_records.tas = award_financial_records.tas
    AND object_class_records.program_activity_code = award_financial_records.program_activity_code
-- For the final five values, the numbers in file B are expected to be larger than those in file C. For the rest,
-- they are expected to be larger in absolute value but negative, therefore farther left on the number line and smaller
-- in numeric value
WHERE ussgl480100_undelivered_or_fyb_sum_c < ussgl480100_undelivered_or_fyb_sum_b
    OR ussgl480100_undelivered_or_cpe_sum_c < ussgl480100_undelivered_or_cpe_sum_b
    OR ussgl483100_undelivered_or_cpe_sum_c < ussgl483100_undelivered_or_cpe_sum_b
    OR ussgl488100_upward_adjustm_cpe_sum_c < ussgl488100_upward_adjustm_cpe_sum_b
    OR obligations_undelivered_or_fyb_sum_c < obligations_undelivered_or_fyb_sum_b
    OR obligations_undelivered_or_cpe_sum_c < obligations_undelivered_or_cpe_sum_b
    OR ussgl490100_delivered_orde_fyb_sum_c < ussgl490100_delivered_orde_fyb_sum_b
    OR ussgl490100_delivered_orde_cpe_sum_c < ussgl490100_delivered_orde_cpe_sum_b
    OR ussgl493100_delivered_orde_cpe_sum_c < ussgl493100_delivered_orde_cpe_sum_b
    OR ussgl498100_upward_adjustm_cpe_sum_c < ussgl498100_upward_adjustm_cpe_sum_b
    OR obligations_delivered_orde_fyb_sum_c < obligations_delivered_orde_fyb_sum_b
    OR obligations_delivered_orde_cpe_sum_c < obligations_delivered_orde_cpe_sum_b
    OR ussgl480200_undelivered_or_fyb_sum_c < ussgl480200_undelivered_or_fyb_sum_b
    OR ussgl480200_undelivered_or_cpe_sum_c < ussgl480200_undelivered_or_cpe_sum_b
    OR ussgl483200_undelivered_or_cpe_sum_c < ussgl483200_undelivered_or_cpe_sum_b
    OR ussgl488200_upward_adjustm_cpe_sum_c < ussgl488200_upward_adjustm_cpe_sum_b
    OR gross_outlays_undelivered_fyb_sum_c < gross_outlays_undelivered_fyb_sum_b
    OR gross_outlays_undelivered_cpe_sum_c < gross_outlays_undelivered_cpe_sum_b
    OR ussgl490200_delivered_orde_cpe_sum_c < ussgl490200_delivered_orde_cpe_sum_b
    OR ussgl490800_authority_outl_fyb_sum_c < ussgl490800_authority_outl_fyb_sum_b
    OR ussgl490800_authority_outl_cpe_sum_c < ussgl490800_authority_outl_cpe_sum_b
    OR ussgl498200_upward_adjustm_cpe_sum_c < ussgl498200_upward_adjustm_cpe_sum_b
    OR gross_outlays_delivered_or_fyb_sum_c < gross_outlays_delivered_or_fyb_sum_b
    OR gross_outlays_delivered_or_cpe_sum_c < gross_outlays_delivered_or_cpe_sum_b
    OR gross_outlay_amount_by_awa_fyb_sum_c < gross_outlay_amount_by_pro_fyb_sum_b
    OR gross_outlay_amount_by_awa_cpe_sum_c < gross_outlay_amount_by_pro_cpe_sum_b
    OR obligations_incurred_byawa_cpe_sum_c < obligations_incurred_by_pr_cpe_sum_b
    OR ussgl487100_downward_adjus_cpe_sum_c > ussgl487100_downward_adjus_cpe_sum_b
    OR ussgl497100_downward_adjus_cpe_sum_c > ussgl497100_downward_adjus_cpe_sum_b
    OR ussgl487200_downward_adjus_cpe_sum_c > ussgl487200_downward_adjus_cpe_sum_b
    OR ussgl497200_downward_adjus_cpe_sum_c > ussgl497200_downward_adjus_cpe_sum_b
    OR deobligations_recov_by_awa_cpe_sum_c > deobligations_recov_by_pro_cpe_sum_b;
