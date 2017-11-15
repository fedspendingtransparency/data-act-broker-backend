-- DeobligationsRecoveriesRefundsOfPriorYearByAward_CPE = USSGL(4871+ 4872 + 4971 + 4972).
-- This applies to the award level.
SELECT
	row_number,
	deobligations_recov_by_awa_cpe,
	ussgl487100_downward_adjus_cpe,
	ussgl487200_downward_adjus_cpe,
	ussgl497100_downward_adjus_cpe,
	ussgl497200_downward_adjus_cpe
FROM award_financial
WHERE submission_id = {0}
    AND COALESCE(deobligations_recov_by_awa_cpe,0) <>
        COALESCE(ussgl487100_downward_adjus_cpe,0) +
        COALESCE(ussgl487200_downward_adjus_cpe,0) +
        COALESCE(ussgl497100_downward_adjus_cpe,0) +
        COALESCE(ussgl497200_downward_adjus_cpe,0);
