-- When both are provided, IndirectCostFederalShareAmount should be less than or equal to FederalActionObligation.
SELECT
    row_number,
    indirect_federal_sharing,
    federal_action_obligation,
    afa_generated_unique AS "uniqueid_AssistanceTransactionUniqueKey"
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND COALESCE(indirect_federal_sharing, 0) <> 0
    AND federal_action_obligation IS NOT NULL
    AND (federal_action_obligation = 0
        OR federal_action_obligation * indirect_federal_sharing < 0
        OR ABS(federal_action_obligation) < ABS(indirect_federal_sharing))
    AND UPPER(COALESCE(correction_delete_indicatr, '')) <> 'D';
