SELECT
    row_number,
    gross_outlays_undelivered_cpe,
    ussgl480200_undelivered_or_cpe,
    ussgl488200_upward_adjustm_cpe
FROM object_class_program_activity
WHERE submission_id = {} AND
    COALESCE(gross_outlays_undelivered_cpe, 0) <>
        (COALESCE(ussgl480200_undelivered_or_cpe, 0) +
        COALESCE(ussgl488200_upward_adjustm_cpe, 0))