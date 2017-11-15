-- The unique combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must exist as
-- a currently published record when the record is a deletion (i.e., if CorrectionLateDeleteIndicator = D).
SELECT
    dafa.row_number,
    dafa.fain,
    dafa.award_modification_amendme,
    dafa.uri,
    dafa.awarding_sub_tier_agency_c,
    dafa.correction_late_delete_ind
FROM detached_award_financial_assistance as dafa
WHERE dafa.submission_id = {0}
    AND COALESCE(UPPER(dafa.correction_late_delete_ind), '') = 'D'
    AND NOT EXISTS (
        SELECT 1
        FROM published_award_financial_assistance AS pafa
        WHERE dafa.afa_generated_unique = pafa.afa_generated_unique
            AND pafa.is_active = TRUE
    );
