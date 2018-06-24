from io import StringIO
from unittest.mock import patch
import datetime

from dataactvalidator.scripts import load_program_activity
from dataactcore.models.domainModels import ProgramActivity, ExternalDataType


@patch('dataactvalidator.scripts.load_program_activity.io.BytesIO')
@patch('dataactvalidator.scripts.load_program_activity.boto3')
def test_get_program_activity_file_aws(boto3, bytesIO, monkeypatch):

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': True})

    bytesIO.return_value = None

    load_program_activity.get_program_activity_file('some path')

    boto3.resource('s3').Object(
            load_program_activity.PA_BUCKET, load_program_activity.PA_SUB_KEY+load_program_activity.PA_FILE_NAME
            ).get.assert_called_with(load_program_activity.PA_SUB_KEY+load_program_activity.PA_FILE_NAME)


def test_get_program_activity_file_local(monkeypatch):

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    pa_file = load_program_activity.get_program_activity_file('local_path')

    assert pa_file == 'local_path/DATA Act Program Activity List for Treas.csv'


def test_set_get_pa_last_upload_existing(monkeypatch, database):
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    sess = database.session
    pa_data_type = ExternalDataType(external_data_type_id=2, name="program_activity_upload", description="lorem ipsum")
    sess.add(pa_data_type)
    sess.commit()

    load_program_activity.set_stored_pa_last_upload(
            load_program_activity.make_date_tz_aware(datetime.datetime(2017, 12, 31, 0, 0, 0))
        )

    stored_date = load_program_activity.make_date_tz_aware(load_program_activity.get_stored_pa_last_upload())

    assert stored_date == load_program_activity.make_date_tz_aware(datetime.datetime(2017, 12, 31, 0, 0, 0))

    # repeat this, because the first time, there is no stored object, but now test with one that already exists.

    load_program_activity.set_stored_pa_last_upload(
            load_program_activity.make_date_tz_aware(datetime.datetime(2016, 12, 31, 0, 0, 0))
        )

    stored_date = load_program_activity.make_date_tz_aware(load_program_activity.get_stored_pa_last_upload())

    assert stored_date == load_program_activity.make_date_tz_aware(datetime.datetime(2016, 12, 31, 0, 0, 0))


@patch('dataactvalidator.scripts.load_program_activity.set_stored_pa_last_upload')
@patch('dataactvalidator.scripts.load_program_activity.get_stored_pa_last_upload')
@patch('dataactvalidator.scripts.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactvalidator.scripts.load_program_activity.get_program_activity_file')
def test_load_program_activity_data(mocked_get_pa_file,
                                    mocked_get_current_date,
                                    mocked_get_stored_date,
                                    mocked_set_stored_date,
                                    database,
                                    monkeypatch):
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    mocked_get_pa_file.return_value = StringIO(
        """YEAR,AGENCY_ID,ALLOCATION_ID,ACCOUNT,PA_CODE,PA_NAME,FYQ\n2000,000,111,0000,1111,Test Name,FY2015Q1"""
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    sess = database.session

    load_program_activity.load_program_activity_data('some_path')

    pa = sess.query(ProgramActivity).one_or_none()

    assert pa.fiscal_year_quarter == 'FY2015Q1'
    assert pa.agency_id == '000'
    assert pa.allocation_transfer_id == '111'
    assert pa.account_number == '0000'
    assert pa.program_activity_code == '1111'
    assert pa.program_activity_name == 'test name'
