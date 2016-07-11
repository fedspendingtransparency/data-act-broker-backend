SELECT
	row_number,
	deobligations_recov_by_awa_cpe,
	ussgl487100_downward_adjus_cpe,
	ussgl487200_downward_adjus_cpe,
	ussgl497100_downward_adjus_cpe,
	ussgl497200_downward_adjus_cpe
FROM award_financial
WHERE submission_id = {}
AND COALESCE(deobligations_recov_by_awa_cpe,0) <>
	COALESCE(ussgl487100_downward_adjus_cpe,0) +
	COALESCE(ussgl487200_downward_adjus_cpe,0) +
	COALESCE(ussgl497100_downward_adjus_cpe,0) +
	COALESCE(ussgl497200_downward_adjus_cpe,0);