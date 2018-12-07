UPDATE published_award_financial_assistance AS pafa
SET awarding_office_name = office.office_name
FROM office
WHERE pafa.awarding_office_code = office.office_code
  AND cast_as_date(pafa.action_date) >= '2018/10/01'
  AND pafa.awarding_office_name IS NULL
  AND pafa.awarding_office_code IS NOT NULL;
