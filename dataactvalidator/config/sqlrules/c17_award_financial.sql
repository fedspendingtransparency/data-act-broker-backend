SELECT row_number, transaction_obligated_amou
FROM award_financial
WHERE submission_id = {}
	AND transaction_obligated_amou IS NULL
	AND (COALESCE(ussgl480100_undelivered_or_cpe,0) = 0
		AND COALESCE(ussgl480100_undelivered_or_fyb,0) = 0
		AND COALESCE(ussgl480200_undelivered_or_cpe,0) = 0
		AND COALESCE(ussgl480200_undelivered_or_fyb,0) = 0
		AND COALESCE(ussgl483100_undelivered_or_cpe,0) = 0
		AND COALESCE(ussgl483200_undelivered_or_cpe,0) = 0
		AND COALESCE(ussgl487100_downward_adjus_cpe,0) = 0
		AND COALESCE(ussgl487200_downward_adjus_cpe,0) = 0
		AND COALESCE(ussgl488100_upward_adjustm_cpe,0) = 0
		AND COALESCE(ussgl488200_upward_adjustm_cpe,0) = 0
		AND COALESCE(ussgl490100_delivered_orde_cpe,0) = 0
		AND COALESCE(ussgl490100_delivered_orde_fyb,0) = 0
		AND COALESCE(ussgl490200_delivered_orde_cpe,0) = 0
		AND COALESCE(ussgl490800_authority_outl_cpe,0) = 0
		AND COALESCE(ussgl490800_authority_outl_fyb,0) = 0
		AND COALESCE(ussgl493100_delivered_orde_cpe,0) = 0
		AND COALESCE(ussgl497100_downward_adjus_cpe,0) = 0
		AND COALESCE(ussgl497200_downward_adjus_cpe,0) = 0
		AND COALESCE(ussgl498100_upward_adjustm_cpe,0) = 0
		AND COALESCE(ussgl498200_upward_adjustm_cpe,0) = 0);