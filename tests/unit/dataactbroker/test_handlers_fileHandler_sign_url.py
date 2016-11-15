from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactbroker.handlers import fileHandler
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from unittest.mock import Mock
import json

def test_get_signed_url_for_submission_file_local(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    interfaces = InterfaceHolder()
    file_handler = fileHandler.FileHandler(Mock(), interfaces=interfaces, isLocal=True, serverPath="/test/server/path/")
    file_handler.check_submission_permission = Mock()

    mock_dict = Mock()
    mock_dict.return_value.getValue.side_effect = ['file_name', str(submission.submission_id)]
    monkeypatch.setattr(fileHandler, 'RequestDictionary', mock_dict)

    json_response = file_handler.get_signed_url_for_submission_file()
    print(json.loads(json_response.get_data().decode("utf-8")))
    assert json.loads(json_response.get_data().decode("utf-8"))['url'] == "/test/server/path/file_name.csv"

def test_get_signed_url_for_submission_file_s3(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    interfaces = InterfaceHolder()
    file_handler = fileHandler.FileHandler(Mock(), interfaces=interfaces, isLocal=False)
    file_handler.check_submission_permission = Mock()

    mock_dict = Mock()
    mock_dict.return_value.getValue.side_effect = ['file_name', str(submission.submission_id)]
    monkeypatch.setattr(fileHandler, 'RequestDictionary', mock_dict)

    mock_dict = Mock()
    mock_dict.return_value.getSignedUrl.side_effect = ['/signed/url/path/file_name.csv']
    monkeypatch.setattr(fileHandler, 's3UrlHandler', mock_dict)
    file_handler.s3manager.getSignedUrl = Mock()

    json_response = file_handler.get_signed_url_for_submission_file()
    assert json.loads(json_response.get_data().decode("utf-8"))['url'] == '/signed/url/path/file_name.csv'