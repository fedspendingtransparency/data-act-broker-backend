-- DeobligationsRecoveriesRefundsByTAS_CPE in File A (appropriation) should equal USSGL
-- (4871_CPE + 4971_CPE + 4872_CPE + 4972_CPE) for the TAS in File B (object class program activity).
WITH appropriation_a35_{0} AS
    (SELECT row_number,
        deobligations_recoveries_r_cpe,
        tas_id
    FROM appropriation
    WHERE submission_id = {0}),
ocpa_a35_{0} AS
    (SELECT tas_id,
        ussgl487100_downward_adjus_cpe,
        ussgl497100_downward_adjus_cpe,
        ussgl487200_downward_adjus_cpe,
        ussgl497200_downward_adjus_cpe
    FROM object_class_program_activity
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.deobligations_recoveries_r_cpe,
    SUM(op.ussgl487100_downward_adjus_cpe) AS ussgl487100_downward_adjus_cpe_sum,
    SUM(op.ussgl497100_downward_adjus_cpe) AS ussgl497100_downward_adjus_cpe_sum,
    SUM(op.ussgl487200_downward_adjus_cpe) AS ussgl487200_downward_adjus_cpe_sum,
    SUM(op.ussgl497200_downward_adjus_cpe) AS ussgl497200_downward_adjus_cpe_sum
FROM appropriation_a35_{0} AS approp
    JOIN ocpa_a35_{0} AS op
        ON approp.tas_id = op.tas_id
GROUP BY approp.row_number,
    approp.deobligations_recoveries_r_cpe
HAVING approp.deobligations_recoveries_r_cpe <>
        SUM(op.ussgl487100_downward_adjus_cpe) +
        SUM(op.ussgl497100_downward_adjus_cpe) +
        SUM(op.ussgl487200_downward_adjus_cpe) +
        SUM(op.ussgl497200_downward_adjus_cpe);

