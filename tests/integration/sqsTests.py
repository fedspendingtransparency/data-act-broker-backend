from dataactcore.aws.sqsHandler import sqs_queue
from dataactvalidator.sqs_work_dispatcher import SQSWorkDispatcher
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

    def test_sqs_work_dispatched_with_numeric_message_body(self):
        """SQSWorkDispatcher can execute work on a numeric message body successfully
        Given a numeric message body
        When on a SQSWorkDispatcher.dispatch() is called
        Then the default message_transformer provides it as an argument to the job
        """
        queue = sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                       monitor_sleep_time=1)

        def do_some_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work)
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)
