WITH submission_uei_{0} AS
    (SELECT awardee_or_recipient_uei
    FROM (
        SELECT DISTINCT awardee_or_recipient_uei
        FROM award_procurement
        WHERE submission_id = {0}
        UNION
        SELECT DISTINCT awardee_or_recipient_uei
        FROM award_financial_assistance
        WHERE submission_id = {0}) AS temp)
SELECT
    DISTINCT ON (uei)
    uei AS "AwardeeOrRecipientUEI",
    legal_business_name AS "AwardeeOrRecipientLegalEntityName",
    ultimate_parent_uei AS "UltimateParentUEI",
    ultimate_parent_legal_enti AS "UltimateParentLegalEntityName",
    high_comp_officer1_full_na AS "HighCompOfficer1FullName",
    high_comp_officer1_amount AS "HighCompOfficer1Amount",
    high_comp_officer2_full_na AS "HighCompOfficer2FullName",
    high_comp_officer2_amount AS "HighCompOfficer2Amount",
    high_comp_officer3_full_na AS "HighCompOfficer3FullName",
    high_comp_officer3_amount AS "HighCompOfficer3Amount",
    high_comp_officer4_full_na AS "HighCompOfficer4FullName",
    high_comp_officer4_amount AS "HighCompOfficer4Amount",
    high_comp_officer5_full_na AS "HighCompOfficer5FullName",
    high_comp_officer5_amount AS "HighCompOfficer5Amount"
FROM sam_recipient
WHERE EXISTS (
    SELECT 1
    FROM submission_uei_{0} AS sd
    WHERE sd.awardee_or_recipient_uei = sam_recipient.uei
)
ORDER BY uei, sam_recipient_id DESC
