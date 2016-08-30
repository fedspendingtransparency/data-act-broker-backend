from dataactcore.utils.jobQueue import JobQueue
from dataactbroker.handlers.interfaceHolder import InterfaceHolder
from unittest.mock import Mock

def test_generate_d_file_success(monkeypatch):
    """ Test successful generation of D1 and D2 files """
    # with open("test_file.csv", "w") as file:
    #     file.write("test")
    #
    # result_xml = "<results>test_file.csv</results>"
    #
    # jq = JobQueue()
    # monkeypatch.setattr(jq, 'get_xml_response_content', Mock(return_value=result_xml))
    #
    # jq.generate_d_file('', 1, 1, InterfaceHolder, '12345_test_file.csv', True)
    pass


def test_generate_d_file_failure(monkeypatch):
    """ Test unsuccessful generation of D1 and D2 files """
    pass