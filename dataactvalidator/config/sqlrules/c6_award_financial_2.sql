-- GrossOutlaysUndeliveredOrdersPrepaidTotal (FYB) = USSGL(4802 + 4882). This applies to the award level.
SELECT
	row_number,
	gross_outlays_undelivered_fyb,
	ussgl480200_undelivered_or_fyb
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_undelivered_fyb,0) <> COALESCE(ussgl480200_undelivered_or_fyb,0);
