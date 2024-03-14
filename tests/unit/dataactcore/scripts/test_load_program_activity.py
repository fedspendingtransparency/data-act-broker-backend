from io import StringIO
from unittest.mock import patch
import datetime
import pytest
import os

from dataactcore.scripts.pipeline import load_program_activity
from dataactcore.interfaces import function_bag
from dataactcore.models.domainModels import ProgramActivity, ExternalDataType


def remove_metrics_file():
    if os.path.isfile('load_program_activity_metrics.json'):
        os.remove('load_program_activity_metrics.json')


def remove_exported_file():
    if os.path.isfile('program_activity.csv'):
        os.remove('program_activity.csv')


def add_relevant_data_types(sess):
    data_types = sess.query(ExternalDataType).all()
    if len(data_types) == 0:
        pa_upload = ExternalDataType(external_data_type_id=2, name="program_activity_upload", description="lorem ipsum")
        pa = ExternalDataType(external_data_type_id=16, name="program_activity", description="lorem ipsum")
        sess.add_all([pa, pa_upload])
        sess.commit()


@patch('dataactcore.scripts.pipeline.load_program_activity.io.BytesIO')
@patch('dataactcore.scripts.pipeline.load_program_activity.boto3')
def test_get_program_activity_file_aws(boto3, bytesio, monkeypatch):
    """ Test retrieving the program activity file from AWS """

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': True, 'aws_region': 'region'})

    bytesio.return_value = None

    load_program_activity.get_program_activity_file('some path')

    boto3.resource('s3').Object(
        load_program_activity.PA_BUCKET, load_program_activity.PA_SUB_KEY + load_program_activity.PA_FILE_NAME
    ).get.assert_called_with(Key=load_program_activity.PA_SUB_KEY + load_program_activity.PA_FILE_NAME)

    remove_metrics_file()


def test_get_program_activity_file_local(monkeypatch):
    """ Test obtaining the local file based on its path """

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    pa_file = load_program_activity.get_program_activity_file('local_path')

    assert pa_file == 'local_path/DATA Act Program Activity List for Treas.csv'

    remove_metrics_file()


@patch('dataactcore.interfaces.function_bag.update_external_data_load_date')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_stored_pa_last_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_program_activity_file')
def test_export_program_activity(mocked_get_pa_file, mocked_get_current_date, mocked_get_stored_date,
                                 mocked_set_stored_date, database, monkeypatch):
    """ Test exporting the program activity """

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    mocked_get_pa_file.return_value = StringIO(
        "AGENCY,OMB_BUREAU_TITLE_OPTNL,OMB_ACCOUNT_TITLE_OPTNL,AGENCY_CODE,ALLOCATION_ID,ACCOUNT_CODE,PA_CODE,"
        "PA_TITLE,FYQ\n"
        "Test Agency,Test Office,Test Account,2000,000,111,0000,1111,Test Name,FY15P03\n"
        "Test Agency,Test Office,Test Account,2000,000,111,0000,1111,Test Name 2,FY15Q2"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    sess = database.session
    add_relevant_data_types(sess)

    load_program_activity.load_program_activity_data('some_path', force_reload=True, export=True)

    export_path = 'program_activity.csv'
    with open(export_path, 'r') as export_pa:
        actual_headers = export_pa.readline()
    remove_exported_file()

    expected_headers = ('REPORTING_PERIOD,AGENCY_IDENTIFIER_NAME,ALLOCATION_TRANSFER_AGENCY_IDENTIFIER_CODE,'
                        'AGENCY_IDENTIFIER_CODE,MAIN_ACCOUNT_CODE,PROGRAM_ACTIVITY_NAME,PROGRAM_ACTIVITY_CODE,'
                        'OMB_BUREAU_TITLE_OPTNL,OMB_ACCOUNT_TITLE_OPTNL\n')
    assert expected_headers == actual_headers

    remove_metrics_file()


def test_set_get_pa_last_upload_existing(monkeypatch, database):
    """ Test the last upload date/time retrieval """

    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    add_relevant_data_types(database.session)

    # test epoch timing
    stored_date = load_program_activity.get_stored_pa_last_upload()
    expected_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert stored_date == expected_date

    function_bag.update_external_data_load_date(datetime.datetime(2017, 12, 31, 0, 0, 0),
                                                datetime.datetime(2017, 12, 31, 0, 0, 0), 'program_activity_upload')

    stored_date = load_program_activity.get_stored_pa_last_upload()
    expected_date = datetime.datetime(2017, 12, 31, 0, 0, 0)
    assert stored_date == expected_date

    # repeat this, because the first time, there is no stored object, but now test with one that already exists.
    function_bag.update_external_data_load_date(datetime.datetime(2016, 12, 31, 0, 0, 0),
                                                datetime.datetime(2016, 12, 31, 0, 0, 0), 'program_activity_upload')

    stored_date = load_program_activity.get_stored_pa_last_upload()
    expected_date = datetime.datetime(2016, 12, 31, 0, 0, 0)
    assert stored_date == expected_date

    remove_metrics_file()


@patch('dataactcore.interfaces.function_bag.update_external_data_load_date')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_stored_pa_last_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_program_activity_file')
def test_load_program_activity_data(mocked_get_pa_file, mocked_get_current_date, mocked_get_stored_date,
                                    mocked_set_stored_date, database, monkeypatch):
    """ Test actually loading the program activity data """
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})

    mocked_get_pa_file.return_value = StringIO(
        "AGENCY,OMB_BUREAU_TITLE_OPTNL,OMB_ACCOUNT_TITLE_OPTNL,AGENCY_CODE,ALLOCATION_ID,ACCOUNT_CODE,PA_CODE,"
        "PA_TITLE,FYQ\n"
        "Test Agency,Test Office,Test Account,2000,000,111,0000,1111,Test Name,FY15P03\n"
        "Test Agency,Test Office,Test Account,2000,000,111,0000,1111,Test Name 2,FY15Q2"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    sess = database.session
    add_relevant_data_types(sess)

    load_program_activity.load_program_activity_data('some_path')

    pa = sess.query(ProgramActivity).filter(ProgramActivity.program_activity_name == 'test name').one_or_none()

    assert pa.fiscal_year_period == 'FY15P03'
    assert pa.agency_id == '000'
    assert pa.allocation_transfer_id == '111'
    assert pa.account_number == '0000'
    assert pa.program_activity_code == '1111'
    assert pa.program_activity_name == 'test name'

    pa = sess.query(ProgramActivity).filter(ProgramActivity.program_activity_name == 'test name 2').one_or_none()

    assert pa.fiscal_year_period == 'FY15P06'

    remove_metrics_file()


@patch('dataactcore.interfaces.function_bag.update_external_data_load_date')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_stored_pa_last_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_program_activity_file')
def test_load_program_activity_data_only_header(mocked_get_pa_file, mocked_get_current_date, mocked_get_stored_date,
                                                mocked_set_stored_date, monkeypatch):
    """ Test actually loading the program activity data """
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    monkeypatch.setattr(function_bag, 'CONFIG_BROKER', {'local': False})

    mocked_get_pa_file.return_value = StringIO(
        "AGENCY,OMB_BUREAU_TITLE_OPTNL,OMB_ACCOUNT_TITLE_OPTNL,AGENCY_CODE,ALLOCATION_ID,ACCOUNT_CODE,PA_CODE,"
        "PA_TITLE,FYQ"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    with pytest.raises(SystemExit) as se:
        load_program_activity.load_program_activity_data('some_path')

    assert se.value.code == 4

    remove_metrics_file()


@patch('dataactcore.interfaces.function_bag.update_external_data_load_date')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_stored_pa_last_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_program_activity_file')
def test_load_program_activity_data_no_header(mocked_get_pa_file, mocked_get_current_date, mocked_get_stored_date,
                                              mocked_set_stored_date, monkeypatch):
    """ Test actually loading the program activity data """
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    monkeypatch.setattr(function_bag, 'CONFIG_BROKER', {'local': False})

    mocked_get_pa_file.return_value = StringIO(
        "Test Agency,Test Office,Test Account,2000,000,111,0000,1111,Test Name,FY15Q1"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    with pytest.raises(SystemExit) as se:
        load_program_activity.load_program_activity_data('some_path')

    assert se.value.code == 4

    remove_metrics_file()


@patch('dataactcore.interfaces.function_bag.update_external_data_load_date')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_stored_pa_last_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_date_of_current_pa_upload')
@patch('dataactcore.scripts.pipeline.load_program_activity.get_program_activity_file')
def test_load_program_activity_data_empty_file(mocked_get_pa_file, mocked_get_current_date, mocked_get_stored_date,
                                               mocked_set_stored_date, monkeypatch):
    """ Test actually loading the program activity data """
    monkeypatch.setattr(load_program_activity, 'CONFIG_BROKER', {'use_aws': False})
    monkeypatch.setattr(function_bag, 'CONFIG_BROKER', {'local': False})

    mocked_get_pa_file.return_value = StringIO("")

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    with pytest.raises(SystemExit) as se:
        load_program_activity.load_program_activity_data('some_path')

    assert se.value.code == 4

    remove_metrics_file()
