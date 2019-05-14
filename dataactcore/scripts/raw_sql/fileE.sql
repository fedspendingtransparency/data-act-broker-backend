WITH submission_duns AS
    (SELECT awardee_or_recipient_uniqu
    FROM (
        SELECT DISTINCT awardee_or_recipient_uniqu
        FROM award_procurement
        WHERE submission_id = {0}
        UNION
        SELECT DISTINCT awardee_or_recipient_uniqu
        FROM award_financial_assistance
        WHERE submission_id = {0}) AS temp)
SELECT
    DISTINCT ON (awardee_or_recipient_uniqu)
    awardee_or_recipient_uniqu AS "AwardeeOrRecipientUniqueIdentifier",
    legal_business_name AS "AwardeeOrRecipientLegalEntityName",
    ultimate_parent_unique_ide AS "UltimateParentUniqueIdentifier",
    ultimate_parent_legal_enti AS "UltimateParentLegalEntityName",
    NULL AS "HighCompOfficer1FullName",
    NULL AS "HighCompOfficer1Amount",
    NULL AS "HighCompOfficer2FullName",
    NULL AS "HighCompOfficer2Amount",
    NULL AS "HighCompOfficer3FullName",
    NULL AS "HighCompOfficer3Amount",
    NULL AS "HighCompOfficer4FullName",
    NULL AS "HighCompOfficer4Amount",
    NULL AS "HighCompOfficer5FullName",
    NULL AS "HighCompOfficer5Amount"
FROM duns
WHERE EXISTS (
    SELECT 1
    FROM submission_duns
    WHERE submission_duns.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu
)