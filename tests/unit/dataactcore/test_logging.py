from io import StringIO
import json
import logging

from dataactcore import logging as datalogging


def test_exception_formatting():
    output_collector = StringIO()
    handler = logging.StreamHandler(output_collector)
    handler.formatter = datalogging.DeprecatedJSONFormatter()
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)

    try:
        raise ValueError('Message here')
    except:
        logger.exception('Oh noes')

    message = output_collector.getvalue()

    assert message.startswith('Oh noes')
    message = message[len('Oh noes'):].strip()
    message_json = json.loads(message)
    assert message_json['error_log_type'] == str(ValueError)
    assert message_json['error_log_message'] == 'Message here'
    assert message_json['error_log_trace'] != []
