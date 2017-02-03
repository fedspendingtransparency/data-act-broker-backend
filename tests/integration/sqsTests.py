from dataactcore.aws.sqsHandler import sqs_queue
from tests.integration.baseTestValidator import BaseTestValidator


class SQSTests(BaseTestValidator):

    def test_push_poll_queue(self):
        """ Adds a single message to the queue then retrieves it immediately. Default number of messages
        for retrieval is 1 message. """
        queue = sqs_queue()
        response = queue.send_message(MessageBody="1234")
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        messages = queue.receive_messages(WaitTimeSeconds=10)
        self.assertNotEqual(messages, [])
        for message in messages:
            message.delete()
