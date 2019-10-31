-- ObligationsUndeliveredOrdersUnpaidTotal (FYB) = USSGL(4801 + 4881). This applies to the award level.
SELECT
    row_number,
    obligations_undelivered_or_fyb,
    ussgl480100_undelivered_or_fyb,
    COALESCE(obligations_undelivered_or_fyb, 0) - COALESCE(ussgl480100_undelivered_or_fyb, 0) AS "difference",
    display_tas AS "uniqueid_TAS",
    piid AS "uniqueid_PIID",
    fain AS "uniqueid_FAIN",
    uri AS "uniqueid_URI"
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(obligations_undelivered_or_fyb, 0) <> COALESCE(ussgl480100_undelivered_or_fyb, 0);
