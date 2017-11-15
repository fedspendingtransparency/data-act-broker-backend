-- GrossOutlaysUndeliveredOrdersPrepaidTotal (CPE) = USSGL(4802 + 4882). This applies to the award level.
SELECT
	row_number,
	gross_outlays_undelivered_cpe,
	ussgl480200_undelivered_or_cpe,
	ussgl488200_upward_adjustm_cpe
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(gross_outlays_undelivered_cpe,0) <>
        COALESCE(ussgl480200_undelivered_or_cpe,0) +
        COALESCE(ussgl488200_upward_adjustm_cpe,0);
