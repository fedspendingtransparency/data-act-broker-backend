-- The combination of FAIN, AwardModificationAmendmentNumber, URI, and AwardingSubTierAgencyCode must be unique from
-- currently published ones unless the record is a correction or deletion, if CorrectionLateDeleteIndicator = C or D.
SELECT
    dafa.row_number,
    dafa.fain,
    dafa.award_modification_amendme,
    dafa.uri,
    dafa.awarding_sub_tier_agency_c,
    dafa.correction_late_delete_ind
FROM detached_award_financial_assistance as dafa
    INNER JOIN published_award_financial_assistance as pafa ON dafa.afa_generated_unique = pafa.afa_generated_unique
        AND pafa.is_active = True
WHERE dafa.submission_id = {0}
    AND COALESCE(UPPER(dafa.correction_late_delete_ind), '') NOT IN ('C', 'D')