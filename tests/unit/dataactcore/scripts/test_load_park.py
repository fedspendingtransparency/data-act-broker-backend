from io import StringIO
from unittest.mock import patch
import datetime
import pytest
import os

from dataactcore.scripts.pipeline import load_park
from dataactcore.interfaces import function_bag
from dataactcore.models.domainModels import ProgramActivityPARK, ExternalDataType


def remove_metrics_file():
    if os.path.isfile("load_park_metrics.json"):
        os.remove("load_park_metrics.json")


def remove_exported_file():
    if os.path.isfile("park.csv"):
        os.remove("park.csv")


def add_relevant_data_types(sess):
    data_types = sess.query(ExternalDataType).all()
    if len(data_types) == 0:
        park_upload = ExternalDataType(external_data_type_id=25, name="park_upload", description="lorem ipsum")
        park = ExternalDataType(external_data_type_id=24, name="park", description="lorem ipsum")
        sess.add_all([park, park_upload])
        sess.commit()


@patch("dataactcore.scripts.pipeline.load_park.io.BytesIO")
@patch("dataactcore.scripts.pipeline.load_park.boto3")
def test_get_park_file_aws(boto3, bytesio, monkeypatch):
    """Test retrieving the park file from AWS"""

    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": True, "aws_region": "region"})

    bytesio.return_value = None

    load_park.get_park_file("some path")

    boto3.resource("s3").Object(
        load_park.PARK_BUCKET, load_park.PARK_SUB_KEY + load_park.PARK_FILE_NAME
    ).get.assert_called_with(Key=load_park.PARK_SUB_KEY + load_park.PARK_FILE_NAME)

    remove_metrics_file()


def test_get_park_file_local(monkeypatch):
    """Test obtaining the local file based on its path"""

    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})

    park_file = load_park.get_park_file("local_path")

    assert park_file == "local_path/PARK_PROGRAM_ACTIVITY.csv"

    remove_metrics_file()


@patch("dataactcore.interfaces.function_bag.update_external_data_load_date")
@patch("dataactcore.scripts.pipeline.load_park.get_stored_park_last_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_date_of_current_park_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_park_file")
def test_export_park(
    mocked_get_park_file, mocked_get_current_date, mocked_get_stored_date, mocked_set_stored_date, database, monkeypatch
):
    """Test exporting the PARK"""

    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})
    mocked_get_park_file.return_value = StringIO(
        "FY,PD,ALLOC_XFER_AGENCY,AID,MAIN_ACCT,SUB_ACCT,COMPOUND_KEY,PARK,PARK_NAME,RECORD_UPDATE_TS,FILE_UPDATE_TS\n"
        "2024,09,,000,0100,,PB2018001050100000001,5ZBPXDKABCD,Test,2024-08-19-11.39.26,2024-08-19-11.39.26 V12.7\n"
        "2024,09,,000,0102,,PB2018001059911000001,5ZBQ016EFGH,Test PARK,2024-08-19-11.39.26,2024-08-19-11.39.26 V12.7"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    sess = database.session
    add_relevant_data_types(sess)

    load_park.load_park_data("some_path", force_reload=True, export=True)

    export_path = "park.csv"
    with open(export_path, "r") as export_park:
        actual_headers = export_park.readline()
    remove_exported_file()

    expected_headers = (
        "FISCAL_YEAR,PERIOD,ALLOCATION_TRANSFER_AGENCY_IDENTIFIER_CODE,AGENCY_IDENTIFIER_CODE,"
        "MAIN_ACCOUNT_CODE,SUB_ACCOUNT_CODE,COMPOUND_KEY,PARK_CODE,PARK_NAME,RECORD_UPDATE_TIMESTAMP,"
        "FILE_UPDATE_TIMESTAMP\n"
    )
    assert expected_headers == actual_headers

    remove_metrics_file()


def test_set_get_park_last_upload_existing(monkeypatch, database):
    """Test the last upload date/time retrieval"""

    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})
    add_relevant_data_types(database.session)

    # test epoch timing
    stored_date = load_park.get_stored_park_last_upload()
    expected_date = datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert stored_date == expected_date

    function_bag.update_external_data_load_date(
        datetime.datetime(2017, 12, 31, 0, 0, 0), datetime.datetime(2017, 12, 31, 0, 0, 0), "park_upload"
    )

    stored_date = load_park.get_stored_park_last_upload()
    expected_date = datetime.datetime(2017, 12, 31, 0, 0, 0)
    assert stored_date == expected_date

    # repeat this, because the first time, there is no stored object, but now test with one that already exists.
    function_bag.update_external_data_load_date(
        datetime.datetime(2016, 12, 31, 0, 0, 0), datetime.datetime(2016, 12, 31, 0, 0, 0), "park_upload"
    )

    stored_date = load_park.get_stored_park_last_upload()
    expected_date = datetime.datetime(2016, 12, 31, 0, 0, 0)
    assert stored_date == expected_date

    remove_metrics_file()


@patch("dataactcore.interfaces.function_bag.update_external_data_load_date")
@patch("dataactcore.scripts.pipeline.load_park.get_stored_park_last_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_date_of_current_park_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_park_file")
def test_load_park_data(
    mocked_get_park_file, mocked_get_current_date, mocked_get_stored_date, mocked_set_stored_date, database, monkeypatch
):
    """Test actually loading the PARK data"""
    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})

    mocked_get_park_file.return_value = StringIO(
        "FY,PD,ALLOC_XFER_AGENCY,AID,MAIN_ACCT,SUB_ACCT,COMPOUND_KEY,PARK,PARK_NAME,RECORD_UPDATE_TS,FILE_UPDATE_TS\n"
        "2024,09,,000,0100,,PB2018001050100000001,5ZBPXDKABCD,Test,2024-08-19-11.39.26,2024-08-19-11.39.26 V12.7\n"
        "2024,09,,000,0102,,PB2018001059911000001,5ZBQ016EFGH,Test PARK,2024-08-19-11.39.26,2024-08-19-11.39.26 V12.7"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    sess = database.session
    add_relevant_data_types(sess)

    load_park.load_park_data("some_path")

    park = sess.query(ProgramActivityPARK).filter(ProgramActivityPARK.park_name == "Test").one_or_none()

    assert park.fiscal_year == 2024
    assert park.period == 9
    assert park.agency_id == "000"
    assert park.allocation_transfer_id is None
    assert park.main_account_number == "0100"
    assert park.sub_account_number is None
    assert park.park_code == "5ZBPXDKABCD"
    assert park.park_name == "Test"

    park = sess.query(ProgramActivityPARK).filter(ProgramActivityPARK.park_name == "Test PARK").one_or_none()

    assert park.park_code == "5ZBQ016EFGH"

    remove_metrics_file()


@patch("dataactcore.interfaces.function_bag.update_external_data_load_date")
@patch("dataactcore.scripts.pipeline.load_park.get_stored_park_last_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_date_of_current_park_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_park_file")
def test_load_park_data_no_header(
    mocked_get_park_file, mocked_get_current_date, mocked_get_stored_date, mocked_set_stored_date, monkeypatch
):
    """Test error if there's no headers"""
    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})
    monkeypatch.setattr(function_bag, "CONFIG_BROKER", {"local": False})

    mocked_get_park_file.return_value = StringIO(
        "2024,09,,000,0100,,PB2018001050100000001,5ZBPXDKABCD,Test,2024-08-19-11.39.26,2024-08-19-11.39.26 V12.7"
    )

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    with pytest.raises(ValueError):
        load_park.load_park_data("some_path")

    remove_metrics_file()


@patch("dataactcore.interfaces.function_bag.update_external_data_load_date")
@patch("dataactcore.scripts.pipeline.load_park.get_stored_park_last_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_date_of_current_park_upload")
@patch("dataactcore.scripts.pipeline.load_park.get_park_file")
def test_load_park_data_empty_file(
    mocked_get_park_file, mocked_get_current_date, mocked_get_stored_date, mocked_set_stored_date, monkeypatch
):
    """Test a completely empty PARK file"""
    monkeypatch.setattr(load_park, "CONFIG_BROKER", {"use_aws": False})
    monkeypatch.setattr(function_bag, "CONFIG_BROKER", {"local": False})

    mocked_get_park_file.return_value = StringIO("")

    mocked_get_current_date.return_value = datetime.datetime(2017, 12, 31, 0, 0, 0)
    mocked_get_stored_date.return_value = datetime.datetime(2016, 12, 31, 0, 0, 0)
    mocked_set_stored_date.return_value = None

    with pytest.raises(SystemExit) as se:
        load_park.load_park_data("some_path")

    assert se.value.code == 4

    remove_metrics_file()
