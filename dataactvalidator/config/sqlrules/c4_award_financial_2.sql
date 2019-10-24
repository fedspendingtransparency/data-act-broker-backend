-- ObligationsDeliveredOrdersUnpaidTotal (FYB) = USSGL(4901 + 4981). This applies to the award level.
SELECT
    row_number,
    obligations_delivered_orde_fyb,
    ussgl490100_delivered_orde_fyb,
    COALESCE(obligations_delivered_orde_fyb, 0) - COALESCE(ussgl490100_delivered_orde_fyb, 0) AS "difference",
    tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(obligations_delivered_orde_fyb, 0) <>
        COALESCE(ussgl490100_delivered_orde_fyb, 0);
