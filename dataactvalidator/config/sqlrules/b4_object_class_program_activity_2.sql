-- ObligationsDeliveredOrdersUnpaidTotal (CPE) = USSGL(4901 + 4981). This applies to the program activity and object
-- class level.
SELECT
    row_number,
    obligations_delivered_orde_cpe,
    ussgl490100_delivered_orde_cpe,
    ussgl498100_upward_adjustm_cpe,
    COALESCE(obligations_delivered_orde_cpe, 0) - (COALESCE(ussgl490100_delivered_orde_cpe, 0) +
                                                   COALESCE(ussgl498100_upward_adjustm_cpe, 0)) AS "variance"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(obligations_delivered_orde_cpe, 0) <>
        COALESCE(ussgl490100_delivered_orde_cpe, 0) +
        COALESCE(ussgl498100_upward_adjustm_cpe, 0);
