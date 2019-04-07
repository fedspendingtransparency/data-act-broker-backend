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

    # def test_sqs_work_dispatched(self):
    #     """Tests that the SQSWorkDispatcher can execute work successfully on a child worker process"""
    #     queue = sqs_queue()
    #     queue.send_message(MessageBody="1234")
    #
    #     dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
    #                                    monitor_sleep_time=1)
    #     task_status = "incomplete"
    #
    #     def do_some_work(task_id):
    #         self.assertEquals(task_id, "1234")  # assert the message body is passed in as arg by default
    #         # makes this a closure, closing over the state of the outer var, so it can be updated
    #         task_status = "complete"
    #
    #     dispatcher.dispatch(do_some_work)
