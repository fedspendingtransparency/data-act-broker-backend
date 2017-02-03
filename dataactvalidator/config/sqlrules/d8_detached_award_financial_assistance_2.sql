-- provided fiscal year is current or directly previous fiscal year
SELECT
    row_number,
    fiscal_year_and_quarter_co
FROM detached_award_financial_assistance
WHERE submission_id = {0}
    AND fiscal_year_and_quarter_co IS NOT NULL
    AND fiscal_year_and_quarter_co != ''
    AND EXTRACT(YEAR FROM (CURRENT_DATE + INTERVAL '3 month')) !=
        CASE WHEN fiscal_year_and_quarter_co ~ '^\d\d\d\d'
             THEN substring(fiscal_year_and_quarter_co from '^\d\d\d\d')::integer
             ELSE 0
        END
    AND EXTRACT(YEAR FROM (CURRENT_DATE + INTERVAL '3 month')) - 1 !=
        CASE WHEN fiscal_year_and_quarter_co ~ '^\d\d\d\d'
             THEN substring(fiscal_year_and_quarter_co from '^\d\d\d\d')::integer
             ELSE 0
        END