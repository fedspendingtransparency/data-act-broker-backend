SELECT row_number, transaction_obligated_amou
FROM award_financial
WHERE submission_id = {}
	AND transaction_obligated_amou IS NULL
	AND (ussgl480100_undelivered_or_cpe IS NULL
		AND ussgl480100_undelivered_or_fyb IS NULL
		AND ussgl480200_undelivered_or_cpe IS NULL
		AND ussgl480200_undelivered_or_fyb IS NULL
		AND ussgl483100_undelivered_or_cpe IS NULL
		AND ussgl483200_undelivered_or_cpe IS NULL
		AND ussgl487100_downward_adjus_cpe IS NULL
		AND ussgl487200_downward_adjus_cpe IS NULL
		AND ussgl488100_upward_adjustm_cpe IS NULL
		AND ussgl488200_upward_adjustm_cpe IS NULL
		AND ussgl490100_delivered_orde_cpe IS NULL
		AND ussgl490100_delivered_orde_fyb IS NULL
		AND ussgl490200_delivered_orde_cpe IS NULL
		AND ussgl490800_authority_outl_cpe IS NULL
		AND ussgl490800_authority_outl_fyb IS NULL
		AND ussgl493100_delivered_orde_cpe IS NULL
		AND ussgl497100_downward_adjus_cpe IS NULL
		AND ussgl497200_downward_adjus_cpe IS NULL
		AND ussgl498100_upward_adjustm_cpe IS NULL
		AND ussgl498200_upward_adjustm_cpe IS NULL);