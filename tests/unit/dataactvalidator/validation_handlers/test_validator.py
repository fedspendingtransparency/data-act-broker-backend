import pytest

from unittest.mock import Mock

from dataactcore.models.stagingModels import FlexField
from dataactcore.models.validationModels import RuleSql
from dataactcore.models.jobModels import FileType
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER_ID, RULE_SEVERITY_DICT
from dataactvalidator.validation_handlers import validator
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory


@pytest.mark.usefixtures("job_constants")
def test_relevant_flex_data(database):
    """Verify that we can retrieve multiple flex fields from our data"""
    sess = database.session
    subs = [SubmissionFactory() for _ in range(3)]

    # Three jobs per submission
    jobs = [JobFactory(submission=sub) for sub in subs for _ in range(3)]
    sess.add_all(subs + jobs)
    sess.commit()
    # Set up ten rows of three fields per job
    sess.add_all(
        [
            FlexField(
                submission_id=job.submission_id,
                job_id=job.job_id,
                row_number=row_number,
                header=str(idx),
                cell="cell" * row_number,
            )
            for job in jobs
            for idx in range(3)
            for row_number in range(1, 11)
        ]
    )
    sess.commit()

    failures = [{"row_number": 3}, {"row_number": 7}]
    result = validator.relevant_flex_data(failures, jobs[0].job_id)
    assert {3, 7} == set(result.keys())
    assert len(result[3]) == 3
    # spot check some of the values
    assert result[3][0].header == "0"
    assert result[3][1].cell == "cell" * 3
    assert result[3][2].job_id == jobs[0].job_id
    assert result[7][1].header == "1"
    assert result[7][0].cell == "cell" * 7


def test_failure_row_to_tuple_flex():
    """Verify that flex data gets included in the failure row info"""
    flex_data = {
        2: [FlexField(header="A", cell="a"), FlexField(header="B", cell="b"), FlexField(header="C", cell=None)],
        4: [FlexField(header="A", cell="c"), FlexField(header="B", cell="d"), FlexField(header="C", cell="g")],
    }

    result = validator.failure_row_to_tuple(Mock(), flex_data, [], [], Mock(), {"row_number": 2})
    assert result.field_name == ""
    assert result.flex_fields == "A: a, B: b, C: "
    assert result.failed_value == ""


@pytest.mark.usefixtures("job_constants")
@pytest.mark.usefixtures("validation_constants")
def test_run_only_sensitive_rules(database):
    sess = database.session

    # two rules, one sensitive, one not
    fabs_file_type = sess.query(FileType).filter_by(letter_name="FABS").one()
    rule_fabs_sensitive = RuleSql(
        rule_sql="SELECT 1 AS row_number",
        rule_label="FABS1",
        rule_error_message="first rule",
        query_name="FABS1",
        file_id=FILE_TYPE_DICT_LETTER_ID["FABS"],
        rule_severity_id=RULE_SEVERITY_DICT["warning"],
        rule_cross_file_flag=False,
        category="completeness",
        sensitive=True,
    )
    rule_fabs_not_sensitive = RuleSql(
        rule_sql="SELECT 1 AS row_number",
        rule_label="FABS2",
        rule_error_message="second rule",
        query_name="FABS2",
        file_id=FILE_TYPE_DICT_LETTER_ID["FABS"],
        rule_severity_id=RULE_SEVERITY_DICT["warning"],
        rule_cross_file_flag=False,
        category="completeness",
        sensitive=False,
    )
    sess.add_all([rule_fabs_sensitive, rule_fabs_not_sensitive])

    # normal FABS submission
    normal_fabs_sub = SubmissionFactory(submission_id="1", cgac_code="097")
    normal_fabs_job = JobFactory(job_id="1", submission_id=normal_fabs_sub.submission_id, file_type=fabs_file_type)
    sess.add_all([normal_fabs_sub, normal_fabs_job])

    failures = []
    for failure in validator.validate_file_by_sql(normal_fabs_job, "fabs", {}, batch_results=False):
        failures.append(failure)
    assert len(failures) == 2

    # Excluding Sensitive Rules
    sensitive_fabs_sub = SubmissionFactory(submission_id="2", cgac_code="999")
    sensitive_fabs_job = JobFactory(
        job_id="2", submission_id=sensitive_fabs_sub.submission_id, file_type=fabs_file_type
    )
    sess.add_all([sensitive_fabs_sub, sensitive_fabs_job])

    failures = []
    for failure in validator.validate_file_by_sql(sensitive_fabs_job, "fabs", {}, batch_results=False):
        failures.append(failure)
    assert len(failures) == 1
