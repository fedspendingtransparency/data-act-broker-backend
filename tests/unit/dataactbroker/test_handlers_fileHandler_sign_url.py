from tests.unit.dataactcore.factories.job import SubmissionFactory
from dataactbroker.handlers import fileHandler
from dataactcore.interfaces.interfaceHolder import InterfaceHolder
from unittest.mock import Mock
from flask import Flask

def test_get_signed_url_for_submission_file_local(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    with Flask(__name__).test_request_context('?file=file_name&submission='+str(submission.submission_id)):
        interfaces = InterfaceHolder()
        file_handler = fileHandler.FileHandler(Mock(), interfaces=interfaces, isLocal=True, serverPath="/test/server/path/")
        file_handler.check_submission_permission = Mock()
        monkeypatch.setattr(
            fileHandler, 'send_from_directory', Mock(return_value='send from directory reached')
        )
        assert file_handler.get_signed_url_for_submission_file() == 'send from directory reached'

def test_get_signed_url_for_submission_file_s3(database, monkeypatch):
    submission = SubmissionFactory()
    database.session.add(submission)
    database.session.commit()

    with Flask(__name__).test_request_context('?file=file_name&submission=' + str(submission.submission_id)):
        interfaces = InterfaceHolder()
        file_handler = fileHandler.FileHandler(Mock(), interfaces=interfaces, isLocal=False)
        file_handler.check_submission_permission = Mock()
        monkeypatch.setattr(fileHandler, 's3UrlHandler', Mock())

        assert file_handler.get_signed_url_for_submission_file().status_code == 302