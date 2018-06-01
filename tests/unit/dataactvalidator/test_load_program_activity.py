from io import StringIO
from unittest.mock import patch

from dataactvalidator.scripts import load_program_activity
from dataactcore.models.domainModels import ProgramActivity


@patch('dataactvalidator.scripts.load_program_activity.boto')
def test_get_program_activity_file_aws(boto, monkeypatch):

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': True, 'aws_region': 'aws_region',
                                                                 'sf_133_bucket': 'aws_bucket', })

    load_program_activity.get_program_activity_file('some path')

    boto.s3.connect_to_region().lookup().get_key.assert_called_with('program_activity.csv')


def test_get_program_activity_file_local(monkeypatch):

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    pa_file = load_program_activity.get_program_activity_file('local_path')

    assert pa_file == 'local_path/program_activity.csv'


@patch('dataactvalidator.scripts.load_program_activity.get_program_activity_file')
def test_load_program_activity_data(mocked_get_pa_file, database, monkeypatch):
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    mocked_get_pa_file.return_value = StringIO(
        """YEAR,AGENCY_ID,ALLOC_ID,ACCOUNT,PA_CODE,PA_NAME,FYQ\n2000,000,111,0000,1111,Test Name,FYQ"""
    )

    sess = database.session

    load_program_activity.load_program_activity_data('some_path')

    pa = sess.query(ProgramActivity).one_or_none()

    assert pa.fiscal_year_quarter == 'FYQ'
    assert pa.agency_id == '000'
    assert pa.allocation_transfer_id == '111'
    assert pa.account_number == '0000'
    assert pa.program_activity_code == '1111'
    assert pa.program_activity_name == 'test name'
