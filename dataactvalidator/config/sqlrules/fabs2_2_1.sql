-- The combination of FAIN, AwardModificationAmendmentNumber, URI, AssistanceListingNumber, and
-- AwardingSubTierAgencyCode must be unique from currently published ones unless the record is a correction or deletion,
-- if CorrectionDeleteIndicator = C or D.
SELECT
    fabs.row_number,
    fabs.fain,
    fabs.award_modification_amendme,
    fabs.uri,
    fabs.awarding_sub_tier_agency_c,
    fabs.assistance_listing_number,
    fabs.correction_delete_indicatr,
    fabs.afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM fabs
    INNER JOIN published_fabs AS pf
        ON UPPER(fabs.afa_generated_unique) = UPPER(pf.afa_generated_unique)
        AND pf.is_active = TRUE
WHERE fabs.submission_id = {0}
    AND COALESCE(UPPER(fabs.correction_delete_indicatr), '') NOT IN ('C', 'D');
