from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactbroker.handlers import fileHandler
from unittest.mock import Mock
import json

def test_get_signed_url_for_submission_file_local(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    file_handler = fileHandler.FileHandler(Mock(), isLocal=True, serverPath="/test/server/path/")
    monkeypatch.setattr(fileHandler, 'RequestDictionary', Mock(derive=Mock(
        return_value={'file': 'file_name'}
    )))

    json_response = file_handler.get_signed_url_for_submission_file(submission)
    assert json.loads(json_response.get_data().decode("utf-8"))['url'] == "/test/server/path/file_name.csv"

def test_get_signed_url_for_submission_file_s3(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    file_handler = fileHandler.FileHandler(Mock(), isLocal=False)
    monkeypatch.setattr(fileHandler, 'RequestDictionary', Mock(derive=Mock(
        return_value={'file': 'file_name'}
    )))

    mock_dict = Mock()
    mock_dict.return_value.getSignedUrl.return_value = '/signed/url/path/file_name.csv'
    monkeypatch.setattr(fileHandler, 's3UrlHandler', mock_dict)

    json_response = file_handler.get_signed_url_for_submission_file(submission)
    assert json.loads(json_response.get_data().decode("utf-8"))['url'] == '/signed/url/path/file_name.csv'
