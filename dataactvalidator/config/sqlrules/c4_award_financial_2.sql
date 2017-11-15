-- ObligationsDeliveredOrdersUnpaidTotal (FYB) = USSGL(4901 + 4981). This applies to the award level.
SELECT
	row_number,
	obligations_delivered_orde_fyb,
	ussgl490100_delivered_orde_fyb
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(obligations_delivered_orde_fyb,0) <>
        COALESCE(ussgl490100_delivered_orde_fyb,0);
