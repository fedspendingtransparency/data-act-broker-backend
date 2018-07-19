import pytest

from unittest.mock import Mock

from dataactcore.models.lookups import JOB_STATUS_DICT, JOB_TYPE_DICT, FILE_TYPE_DICT
from dataactcore.models.stagingModels import FlexField
from dataactvalidator.validation_handlers import validator
from tests.unit.dataactcore.factories.job import JobFactory, SubmissionFactory


@pytest.mark.usefixtures("job_constants")
def test_relevant_flex_data(database):
    """Verify that we can retrieve multiple flex fields from our data"""
    sess = database.session
    subs = [SubmissionFactory() for _ in range(3)]
    sess.add_all(subs)
    sess.commit()
    # Three jobs per submission
    jobs = [JobFactory(submission_id=sub.submission_id, file_type_id=FILE_TYPE_DICT['appropriations'],
                       job_type_id=JOB_TYPE_DICT['csv_record_validation'], job_status_id=JOB_STATUS_DICT['finished'])
            for sub in subs for _ in range(3)]
    sess.add_all(jobs)
    sess.commit()
    # Set up ten rows of three fields per job
    sess.add_all([
        FlexField(submission_id=job.submission_id, job_id=job.job_id, row_number=row_number, header=str(idx),
                  cell="cell"*row_number)
        for job in jobs for idx in range(3) for row_number in range(1, 11)
    ])
    sess.commit()

    failures = [{'row_number': 3}, {'row_number': 7}]
    result = validator.relevant_flex_data(failures, jobs[0].job_id)
    assert {3, 7} == set(result.keys())
    assert len(result[3]) == 3
    # spot check some of the values
    assert result[3][0].header == '0'
    assert result[3][1].cell == 'cell' * 3
    assert result[3][2].job_id == jobs[0].job_id
    assert result[7][1].header == '1'
    assert result[7][0].cell == 'cell' * 7


def test_failure_row_to_tuple_flex():
    """Verify that flex data gets included in the failure row info"""
    flex_data = {
        2: [FlexField(header='A', cell='a'), FlexField(header='B', cell='b')],
        4: [FlexField(header='A', cell='c'), FlexField(header='B', cell='d')],
    }

    result = validator.failure_row_to_tuple(Mock(), flex_data, [], [], Mock(), {'row_number': 2})
    assert result.field_name == 'A, B'
    assert result.failed_value == 'A: a, B: b'
