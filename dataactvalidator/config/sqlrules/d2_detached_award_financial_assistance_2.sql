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
    INNER JOIN published_award_financial_assistance as pafa ON COALESCE(dafa.fain,'') = COALESCE(pafa.fain,'')
        AND COALESCE(dafa.uri,'') = COALESCE(pafa.uri,'')
        AND COALESCE(dafa.award_modification_amendme,'') = COALESCE(pafa.award_modification_amendme,'')
        AND COALESCE(dafa.awarding_sub_tier_agency_c,'') = COALESCE(pafa.awarding_sub_tier_agency_c,'')
WHERE dafa.submission_id = {0}
    AND ((dafa.correction_late_delete_ind <> 'C'
        AND dafa.correction_late_delete_ind <> 'D')
        OR dafa.correction_late_delete_ind IS NULL)