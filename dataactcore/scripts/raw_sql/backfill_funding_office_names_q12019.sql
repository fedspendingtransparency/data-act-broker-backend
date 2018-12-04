UPDATE published_award_financial_assistance AS pafa
SET funding_office_name = office.office_name
FROM office
WHERE pafa.funding_office_code = office.office_code
  AND cast_as_date(pafa.action_date) >= '2018/10/01'
  AND pafa.funding_office_name IS NULL
  AND pafa.funding_office_code IS NOT NULL;
