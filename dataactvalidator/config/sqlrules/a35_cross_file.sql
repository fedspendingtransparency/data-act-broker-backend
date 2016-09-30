SELECT
    approp.row_number,
    approp.deobligations_recoveries_r_cpe,
    SUM(op.ussgl487100_downward_adjus_cpe) as ussgl487100_downward_adjus_cpe_sum,
    SUM(op.ussgl497100_downward_adjus_cpe) as ussgl497100_downward_adjus_cpe_sum,
    SUM(op.ussgl487200_downward_adjus_cpe) as ussgl487200_downward_adjus_cpe_sum,
    SUM(op.ussgl497200_downward_adjus_cpe) as ussgl497200_downward_adjus_cpe_sum
FROM appropriation AS approp
    JOIN object_class_program_activity op ON approp.tas = op.tas
	    AND approp.submission_id = op.submission_id
WHERE approp.submission_id = {}
GROUP BY approp.row_number, approp.deobligations_recoveries_r_cpe
HAVING approp.deobligations_recoveries_r_cpe <>
        (SUM(op.ussgl487100_downward_adjus_cpe) +
        SUM(op.ussgl497100_downward_adjus_cpe) +
        SUM(op.ussgl487200_downward_adjus_cpe) +
        SUM(op.ussgl497200_downward_adjus_cpe))
