import factory
from factory import fuzzy

from datetime import date

from dataactcore.models import jobModels
from tests.unit.dataactcore.factories.user import UserFactory


class SubmissionFactory(factory.Factory):
    class Meta:
        model = jobModels.Submission

    submission_id = None
    user_id = None
    cgac_code = fuzzy.FuzzyText()
    reporting_start_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    reporting_end_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    reporting_fiscal_year = fuzzy.FuzzyInteger(2010, 2040)
    reporting_fiscal_period = fuzzy.FuzzyInteger(1, 4)
    is_quarter_format = False
    publishable = False
    number_of_errors = 0
    number_of_warnings = 0


class JobStatusFactory(factory.Factory):
    class Meta:
        model = jobModels.JobStatus

    job_status_id = None
    name = fuzzy.FuzzyText()
    description = fuzzy.FuzzyText()


class JobTypeFactory(factory.Factory):
    class Meta:
        model = jobModels.JobType

    job_type_id = None
    name = fuzzy.FuzzyText()
    description = fuzzy.FuzzyText()


class FileTypeFactory(factory.Factory):
    class Meta:
        model = jobModels.FileType

    file_type_id = None
    name = fuzzy.FuzzyText()
    description = fuzzy.FuzzyText()
    letter_name = fuzzy.FuzzyText()


class JobFactory(factory.Factory):
    class Meta:
        model = jobModels.Job

    job_id = None
    filename = fuzzy.FuzzyText()
    job_status = factory.SubFactory(JobStatusFactory)
    job_type = factory.SubFactory(JobTypeFactory)
    submission = factory.SubFactory(SubmissionFactory)
    file_type = factory.SubFactory(FileTypeFactory)
    original_filename = fuzzy.FuzzyText()
    file_size = fuzzy.FuzzyInteger(9999)
    number_of_rows = fuzzy.FuzzyInteger(9999)
    number_of_rows_valid = fuzzy.FuzzyInteger(9999)
    number_of_errors = fuzzy.FuzzyInteger(9999)
    number_of_warnings = fuzzy.FuzzyInteger(9999)
    error_message = fuzzy.FuzzyText()
    start_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    end_date = fuzzy.FuzzyDate(date(2010, 1, 1))


class CertifyHistoryFactory(factory.Factory):
    class Meta:
        model = jobModels.CertifyHistory

    certify_history_id = None
    submission_id = fuzzy.FuzzyInteger(9999)
    submission = factory.SubFactory(SubmissionFactory)
    user_id = fuzzy.FuzzyInteger(9999)
    user = factory.SubFactory(UserFactory)


class CertifiedFilesHistoryFactory(factory.Factory):
    class Meta:
        model = jobModels.CertifiedFilesHistory

    certified_files_history_id = None
    certify_history_id = fuzzy.FuzzyInteger(9999)
    submission_id = fuzzy.FuzzyInteger(9999)
    filename = fuzzy.FuzzyText()
    file_type_id = fuzzy.FuzzyInteger(9999)
    file_type = factory.SubFactory(FileTypeFactory)
    warning_filename = fuzzy.FuzzyText()
    narrative = fuzzy.FuzzyText()


class SubmissionNarrativeFactory(factory.Factory):
    class Meta:
        model = jobModels.SubmissionNarrative

    submission_narrative_id = None
    submission_id = fuzzy.FuzzyInteger(9999)
    submission = factory.SubFactory(SubmissionFactory)
    file_type_id = fuzzy.FuzzyInteger(9999)
    file_type = factory.SubFactory(FileTypeFactory)
    narrative = fuzzy.FuzzyText()
