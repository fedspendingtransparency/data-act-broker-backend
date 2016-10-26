SELECT
    row_number,
    gross_outlays_delivered_or_cpe,
    ussgl490200_delivered_orde_cpe,
    ussgl490800_authority_outl_cpe,
    ussgl498200_upward_adjustm_cpe
FROM object_class_program_activity
WHERE submission_id = {} AND
    COALESCE(gross_outlays_delivered_or_cpe, 0) <>
        (COALESCE(ussgl490200_delivered_orde_cpe, 0) +
        COALESCE(ussgl490800_authority_outl_cpe, 0) +
        COALESCE(ussgl498200_upward_adjustm_cpe, 0))