-- The DeobligationsRecoveriesRefundsOfPriorYearByTAS_CPE amount in the appropriations account file (A) does not equal
-- the sum of the corresponding DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE values in the object
-- class and program activity file (B).
WITH appropriation_a35_{0} AS
    (SELECT row_number,
        deobligations_recoveries_r_cpe,
        account_num,
        display_tas
    FROM appropriation
    WHERE submission_id = {0}),
ocpa_a35_{0} AS
    (SELECT account_num,
        ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe
    FROM object_class_program_activity
    WHERE submission_id = {0})
SELECT
    approp.row_number AS "source_row_number",
    approp.deobligations_recoveries_r_cpe AS "source_value_deobligations_recoveries_r_cpe",
    SUM(op.ussgl487100_downward_adjus_cpe) AS "target_value_ussgl487100_downward_adjus_cpe_sum",
    SUM(op.ussgl497100_downward_adjus_cpe) AS "target_value_ussgl497100_downward_adjus_cpe_sum",
    SUM(op.ussgl487200_downward_adjus_cpe) AS "target_value_ussgl487200_downward_adjus_cpe_sum",
    SUM(op.ussgl497200_downward_adjus_cpe) AS "target_value_ussgl497200_downward_adjus_cpe_sum",
    approp.deobligations_recoveries_r_cpe - (SUM(op.ussgl487100_downward_adjus_cpe) +
                                             SUM(op.ussgl497100_downward_adjus_cpe) +
                                             SUM(op.ussgl487200_downward_adjus_cpe) +
                                             SUM(op.ussgl497200_downward_adjus_cpe)) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a35_{0} AS approp
    JOIN ocpa_a35_{0} AS op
        ON approp.account_num = op.account_num
GROUP BY approp.row_number,
    approp.deobligations_recoveries_r_cpe,
    approp.display_tas
HAVING approp.deobligations_recoveries_r_cpe <>
        SUM(op.ussgl487100_downward_adjus_cpe) +
        SUM(op.ussgl497100_downward_adjus_cpe) +
        SUM(op.ussgl487200_downward_adjus_cpe) +
        SUM(op.ussgl497200_downward_adjus_cpe);

