import inspect
import unittest

import boto3
import logging
import multiprocessing as mp
import os
import signal

from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError
from dataactcore.aws.sqsHandler import sqs_queue
from dataactvalidator.sqs_work_dispatcher import SQSWorkDispatcher, QueueWorkerProcessError
from tests.integration.baseTestValidator import BaseTestValidator
from time import sleep


class SQSWorkDispatcherTests(BaseTestValidator):

    def test_default_dispatch_with_numeric_message_body_succeeds(self):
        """SQSWorkDispatcher can execute work on a numeric message body successfully

        - Given a numeric message body
        - When on a SQSWorkDispatcher.dispatch() is called
        - Then the default message_transformer provides it as an argument to the job
        """
        queue = sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                       long_poll_seconds=1, monitor_sleep_time=1)

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

    def test_additional_job_args_can_be_passed(self):
        """Additional args can be passed to the job to execute"""
        queue = sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                       long_poll_seconds=1, monitor_sleep_time=1)

        def do_some_work(task_id, category):
            self.assertEqual(task_id, 1234)  # assert the message body is passed in as arg by default
            self.assertEqual(category, "easy work")

            # The "work" we're doing is just putting something else on the queue
            queue_in_use = sqs_queue()
            queue_in_use.send_message(MessageBody=9999)

        dispatcher.dispatch(do_some_work, additional_job_args=("easy work",))

        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(9999, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_dispatching_by_message_attribute_succeeds(self):
        """SQSWorkDispatcher can read a message attribute to determine which function to call

        - Given a message with a user-defined message attribute
        - When on a SQSWorkDispatcher.dispatch_by_message_attribute() is called
        - And a message_transformer is given to route execution based on that message attribute
        - Then the correct function is executed
        """
        queue = sqs_queue()
        message_attr = {"work_type": {"DataType": "String", "StringValue": "a"}}
        queue.send_message(MessageBody=1234, MessageAttributes=message_attr)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                       long_poll_seconds=1, monitor_sleep_time=1)

        def one_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "a" on the queue
            queue_in_use = sqs_queue()
            queue_in_use.send_message(MessageBody=1)

        def two_work(task_id):
            self.assertEqual(1234, task_id)  # assert the message body is passed in as arg by default

            # The "work" we're doing is just putting "b" on the queue
            queue_in_use = sqs_queue()
            queue_in_use.send_message(MessageBody=2)

        def work_one_or_two(message):
            msg_attr = message.message_attributes
            if msg_attr and msg_attr.get('work_type', {}).get('StringValue') == 'a':
                # Generating a file
                return one_work, (message.body,)
            else:
                return two_work, (message.body,)

        dispatcher.dispatch_by_message_attribute(work_one_or_two)
        dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Make sure the "a_work" was done
        messages = queue.receive_messages(WaitTimeSeconds=1, MaxNumberOfMessages=10)
        self.assertEqual(1, len(messages))
        self.assertEqual(1, messages[0].body)

        # Worker process should have a successful (0) exitcode
        self.assertEqual(0, dispatcher._worker_process.exitcode)

    def test_faulty_queue_connection_raises_correct_exception(self):
        """When a queue cannot be connected to, it raises the appropriate exception"""
        try:
            client_config = Config(connect_timeout=1, read_timeout=1)  # retries config not in v1.5.x
            sqs = boto3.resource('sqs', config=client_config)  # 'us-gov-west-1')
            queue = sqs.Queue("75f4f422-3866-4e4f-9dc9-5364e3de3eaf")
            dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                           long_poll_seconds=1, monitor_sleep_time=1)
            dispatcher.dispatch(lambda x: x*2)
        except (SystemExit, Exception) as e:
            self.assertIsInstance(e, SystemExit)
            self.assertIsNotNone(e.__cause__)
            self.assertTrue(isinstance(e.__cause__, EndpointConnectionError) or
                            isinstance(e.__cause__, ClientError))

    def test_failed_job_detected(self):
        """SQSWorkDispatcher handles failed work within the child process

        - Given a numeric message body
        - When on a SQSWorkDispatcher.dispatch() is called
        - And the function to execute in the child process fails
        - Then a QueueWorkerProcessError exception is raised
        - And the exit code of the worker process is > 0
        """
        with self.assertRaises(QueueWorkerProcessError) as ctx:
            queue = sqs_queue()
            queue.send_message(MessageBody=1234)

            dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                           long_poll_seconds=1, monitor_sleep_time=1)

            def fail_at_work(task_id):
                raise Exception("failing at this particular job...")

            dispatcher.dispatch(fail_at_work)
            dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Worker process should have a failed (> 0)  exitcode
        self.assertGreater(dispatcher._worker_process.exitcode, 0)

    @unittest.skip("Still need to fix runaway procs due to child proc handling signal. See TODOs")
    def test_terminated_job_triggers_exit_signal_handling(self):
        """The child worker process terminated exits the child process and fires a signal to be handled

        - ...
        """
        #with self.assertRaises(QueueWorkerProcessError) as ctx:
        # TODO:FIX Child process runs endlessly as written

        # TODO: Reason shown by logs here:
        # TODO: It appearsPython signal handling is per Python Interpreter/VM, not per-process
        # TODO: So both parent and child processes (when forked not spawned) will use the same VM and be subject
        # TODO: to the same signal handlers
        # TODO: Need to take this into account: child process receiving a signal will be sent to the SAME handler
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)
        queue = sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process", allow_retries=False,
                                       long_poll_seconds=0, monitor_sleep_time=0.05)

        tq = mp.Queue()

        def worker_terminator(terminate_queue: mp.Queue, sleep_interval=0):
            logger.debug("Started worker_terminator. Waiting for the Queue to surface a PID to be terminated.")
            # Wait until there is a worker in the given dispatcher to kill
            pid = None
            while not pid:
                logger.debug("No work yet to be terminated. Waiting {} seconds".format(sleep_interval))
                sleep(sleep_interval)
                pid = terminate_queue.get()

            # Process is running. Now terminate it with signal.SIGTERM
            logger.debug("Found work to be terminated: Worker PID=[{}]".format(pid))
            os.kill(pid, signal.SIGTERM)
            logger.debug("Terminated worker with PID=[{}] using signal.SIGTERM".format(pid))

        def sleepy_worker(task_id, inter_proc_queue: mp.Queue):
            print("in worker before sleep")
            print("running worker with pid {}".format(os.getpid()))

            # Put PID of worker process in the queue to let worker_terminator proc know what to kill
            inter_proc_queue.put(os.getpid())

            sleep(0.25)
            print("in worker after sleep")

        terminator = mp.Process(name="worker_terminator", target=worker_terminator, args=(tq, 0.05))
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(sleepy_worker, additional_job_args=(tq,))

        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(2.1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            print("Exit code = {}".format(dispatcher._worker_process.exitcode))
            self.assertLess(dispatcher._worker_process.exitcode, 0)
        finally:
            fail_with_runaway_proc = False
            if dispatcher._worker_process.is_alive():
                logger.warning("Dispatched worker process with PID {} did not complete in timeout. "
                               "Killing it.".format(dispatcher._worker_process.pid))
                os.kill(dispatcher._worker_process.pid, signal.SIGKILL)
                fail_with_runaway_proc = True
            if terminator.is_alive():
                logger.warning("Terminator worker process with PID {} did not complete in timeout. "
                               "Killing it.".format(terminator.pid))
                os.kill(terminator.pid, signal.SIGKILL)
                fail_with_runaway_proc = True
            if fail_with_runaway_proc:
                self.fail("Worker or its Terminator did not complete in timeout as expected. Test fails.")




