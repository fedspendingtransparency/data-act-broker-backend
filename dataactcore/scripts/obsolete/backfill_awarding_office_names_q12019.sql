UPDATE published_fabs AS pf
SET awarding_office_name = office.office_name
FROM office
WHERE pf.awarding_office_code = office.office_code
  AND cast_as_date(pf.action_date) >= '2018/10/01'
  AND pf.awarding_office_name IS NULL
  AND pf.awarding_office_code IS NOT NULL;
