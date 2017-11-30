-- ObligationsUndeliveredOrdersUnpaidTotal (FYB) = USSGL(4801 + 4881). This applies to the award level.
SELECT
    row_number,
    obligations_undelivered_or_fyb,
    ussgl480100_undelivered_or_fyb
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(obligations_undelivered_or_fyb, 0) <> COALESCE(ussgl480100_undelivered_or_fyb, 0);
