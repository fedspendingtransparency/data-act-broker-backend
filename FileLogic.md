# File A Generation

File A is generated using SF133 data submitted by agencies. TAS and monetary amounts are gathered using the period and year provided.

## TAS Selection

- Generate using agency code (CGAC or FREC)
- Exclude all financial account TAS (fin ind type 2 = F)
- Bucket by ATA unless ATA is null, with these exceptions:
  - Add TAS associated with FREC = 1137 or DE00, plus 017, 021, 057 ATAs into DOD’s bucket (submits as CGAC 097).
  - Add 016 ATAs into DOL’s bucket (submits as FREC 1601).
  - Add 011 ATAs into Peace Corp’s bucket (submits as FREC 1125).
- If ATA is null, group by AID, with these exceptions:
  - If TAS is associated with FREC that belongs to an agency that submits to the Broker via FREC, add TAS into that agency’s bucket.
  - Add AID = 017, 021, 057 into DOD’s bucket (submits as CGAC 097).
  - Add TAS with AID = 011 to the agency’s bucket whose FREC the TAS is associated with, regardless of whether it is an ‘Agency submitting w/ FREC’. For example, TAS "011-X-0089" is bucketed with Treasury.

## Dollar Amount Derivations

- **TotalBudgetaryResources_CPE**: sum of all line 1910 entries for the TAS
- **BudgetAuthorityAppropriatedAmount_CPE**: sum of all line 1160, 1180, 1260, and 1280 entries for the TAS
- **BudgetAuthorityUnobligatedBalanceBroughtForward_FYB**: sum of all line 1000 entries for the TAS
- **AdjustmentsToUnobligatedBalanceBroughtForward_CPE**: sum of all line 1010-1042 entries for the TAS
- **OtherBudgetaryResourcesAmount_CPE**: sum of all line 1540, 1640, 1340, 1440, 1750, and 1850 entries for the TAS
- **ContractAuthorityAmountTotal_CPE**: sum of all line 1540 and 1640 entries for the TAS
- **BorrowingAuthorityAmountTotal_CPE**: sum of all line 1340 and 1440 entries for the TAS
- **SpendingAuthorityfromOffsettingCollectionsAmountTotal_CPE**: sum of all line 1750 and 1850 entries for the TAS
- **StatusOfBudgetaryResourcesTotal_CPE**: sum of all line 2500 entries for the TAS
- **ObligationsIncurredTotalByTAS_CPE**: sum of all line 2190 entries for the TAS
- **GrossOutlayAmountByTAS_CPE**: sum of all line 3020 entries for the TAS
- **UnobligatedBalance_CPE**: sum of all line 2490 entries for the TAS
- **DeobligationsRecoveriesRefundsByTAS_CPE**: sum of all line 1021 and 1033 entries for the TAS