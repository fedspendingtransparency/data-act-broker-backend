-- GrossOutlaysDeliveredOrdersPaidTotal (FYB) = USSGL 4908. This applies to the program activity and object class level.
SELECT
    row_number,
    gross_outlays_delivered_or_fyb,
    ussgl490800_authority_outl_fyb,
    COALESCE(gross_outlays_delivered_or_fyb, 0) - COALESCE(ussgl490800_authority_outl_fyb, 0) AS "difference"
FROM object_class_program_activity
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_delivered_or_fyb, 0) <> COALESCE(ussgl490800_authority_outl_fyb, 0);
