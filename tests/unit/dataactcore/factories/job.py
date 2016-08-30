import factory
from factory import fuzzy

from datetime import date, datetime, timezone

from dataactcore.models import jobModels


class SubmissionFactory(factory.Factory):
    class Meta:
        model = jobModels.Submission

    submission_id = None
    datetime_utc = fuzzy.FuzzyDateTime(
        datetime(2010, 1, 1, tzinfo=timezone.utc))
    user_id = fuzzy.FuzzyInteger(1, 9999)
    cgac_code = fuzzy.FuzzyText()
    reporting_start_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    reporting_end_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    reporting_fiscal_year = fuzzy.FuzzyInteger(2010, 2040)
    reporting_fiscal_period = fuzzy.FuzzyInteger(1, 4)
    is_quarter_format = False
    publishable = False
    number_of_errors = 0
    number_of_warnings = 0
