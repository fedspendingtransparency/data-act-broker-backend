UPDATE published_fabs AS pf
SET funding_office_name = office.office_name
FROM office
WHERE pf.funding_office_code = office.office_code
  AND cast_as_date(pf.action_date) >= '2018/10/01'
  AND pf.funding_office_name IS NULL
  AND pf.funding_office_code IS NOT NULL;
