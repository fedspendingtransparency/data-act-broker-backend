from dataactcore.aws.sqsHandler import get_queue
from tests.integration.baseTestValidator import BaseTestValidator


class SQSTests(BaseTestValidator):

    def test_push_poll_queue(self):
        """ Adds a single message to the queue then retrieves it immediately. Default number of messages
        for retrieval is 1 message. """
        queue = get_queue()
        response = queue.send_message(MessageBody="Test Message")
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        messages = queue.receive_messages(WaitTimeSeconds=10)
        self.assertNotEqual(messages, [])
        for message in messages:
            message.delete()

        messages = queue.receive_messages(WaitTimeSeconds=10)
        self.assertEqual(messages, [])
