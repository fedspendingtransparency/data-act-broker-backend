import factory
from factory import fuzzy

from datetime import date

from dataactcore.models import jobModels
from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
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
    job_status_id = fuzzy.FuzzyChoice(JOB_STATUS_DICT.values())
    job_type_id = fuzzy.FuzzyChoice(JOB_TYPE_DICT.values())
    submission = factory.SubFactory(SubmissionFactory)
    file_type_id = fuzzy.FuzzyChoice(FILE_TYPE_DICT.values())
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
    warning_filename = fuzzy.FuzzyText()
    narrative = fuzzy.FuzzyText()


class SubmissionNarrativeFactory(factory.Factory):
    class Meta:
        model = jobModels.SubmissionNarrative

    submission_narrative_id = None
    submission_id = fuzzy.FuzzyInteger(9999)
    submission = factory.SubFactory(SubmissionFactory)
    file_type_id = fuzzy.FuzzyInteger(9999)
    narrative = fuzzy.FuzzyText()


class ApplicationTypeFactory(factory.Factory):
    class Meta:
        model = jobModels.ApplicationType

    application_type_id = fuzzy.FuzzyInteger(9999)
    application_name = fuzzy.FuzzyText()


class SubmissionWindowFactory(factory.Factory):
    class Meta:
        model = jobModels.SubmissionWindow

    window_id = fuzzy.FuzzyInteger(9999)
    start_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    end_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    block_certification = False
    message = fuzzy.FuzzyText()
    application_type = factory.SubFactory(ApplicationTypeFactory)


class FileRequestFactory(factory.Factory):
    class Meta:
        model = jobModels.FileRequest

    file_request_id = fuzzy.FuzzyInteger(9999)
    request_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    job_id = fuzzy.FuzzyInteger(9999)
    job = factory.SubFactory(JobFactory)
    parent_job_id = fuzzy.FuzzyInteger(9999)
    start_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    end_date = fuzzy.FuzzyDate(date(2010, 1, 1))
    agency_code = fuzzy.FuzzyText()
    file_type = fuzzy.FuzzyText()
    is_cached_file = fuzzy.FuzzyChoice((False, True))


class RevalidationThresholdFactory(factory.Factory):
    class Meta:
        model = jobModels.RevalidationThreshold

    revalidation_date = fuzzy.FuzzyDate(date(2010, 1, 1))
