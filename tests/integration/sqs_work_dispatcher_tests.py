import inspect
import unittest
import boto3
import logging
import multiprocessing as mp
import os
import signal
import psutil as ps

from random import randint
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError, NoCredentialsError, NoRegionError
from dataactcore.aws.sqsHandler import sqs_queue
from dataactvalidator.sqs_work_dispatcher import SQSWorkDispatcher, QueueWorkerProcessError
from tests.integration.baseTestValidator import BaseTestValidator
from time import sleep


class SQSWorkDispatcherTests(BaseTestValidator):
    def tearDown(self):
        sqs_queue().purge()  # clear any lingering messages in the queue between tests
        super().tearDown()

    def test_default_dispatch_with_numeric_message_body_succeeds(self):
        """SQSWorkDispatcher can execute work on a numeric message body successfully

        - Given a numeric message body
        - When on a SQSWorkDispatcher.dispatch() is called
        - Then the default message_transformer provides it as an argument to the job
        """
        queue = sqs_queue()
        queue.send_message(MessageBody=1234)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
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

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
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

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
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
            # note: connection max retries config not in botocore v1.5.x
            client_config = Config(region_name="us-gov-west-1", connect_timeout=1, read_timeout=1)
            sqs = boto3.resource('sqs', config=client_config)
            queue = sqs.Queue("75f4f422-3866-4e4f-9dc9-5364e3de3eaf")
            dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                           long_poll_seconds=1, monitor_sleep_time=1)
            dispatcher.dispatch(lambda x: x*2)
        except (SystemExit, Exception) as e:
            self.assertIsInstance(e, SystemExit)
            self.assertIsNotNone(e.__cause__)
            self.assertTrue(isinstance(e.__cause__, EndpointConnectionError) or
                            isinstance(e.__cause__, ClientError) or
                            isinstance(e.__cause__, NoRegionError) or
                            isinstance(e.__cause__, NoCredentialsError))

    def test_failed_job_detected(self):
        """SQSWorkDispatcher handles failed work within the child process

        - Given a numeric message body
        - When on a SQSWorkDispatcher.dispatch() is called
        - And the function to execute in the child process fails
        - Then a QueueWorkerProcessError exception is raised
        - And the exit code of the worker process is > 0
        """
        with self.assertRaises(QueueWorkerProcessError):
            queue = sqs_queue()
            queue.send_message(MessageBody=1234)

            dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                           long_poll_seconds=1, monitor_sleep_time=1)

            def fail_at_work(task_id):
                raise Exception("failing at this particular job...")

            dispatcher.dispatch(fail_at_work)
            dispatcher._worker_process.join(5)  # wait at most 5 sec for the work to complete

        # Worker process should have a failed (> 0)  exitcode
        self.assertGreater(dispatcher._worker_process.exitcode, 0)

    @unittest.skip("Test is not provable with asserts. Reading STDOUT proves it, but a place to store shared state "
                   "among processes other than STDOUT was not found to be asserted on.")
    def test_separate_signal_handlers_for_child_process(self):
        def fire_alarm():
            print("firing alarm from PID {}".format(os.getpid()))
            signal.setitimer(signal.ITIMER_REAL, 0.01)  # fire alarm in .01 sec
            sleep(0.015)  # Wait for timer to fire signal.SIGALRM

        fired = None

        def handle_sig(sig, frame):
            nonlocal fired
            fired = [os.getpid(), sig]
            print("handled signal from PID {} with fired = {}".format(os.getpid(), fired))

        signal.signal(signal.SIGALRM, handle_sig)
        fire_alarm()
        self.assertIsNotNone(fired)
        self.assertEqual(os.getpid(), fired[0], "PID of handled signal != this process's PID")
        self.assertEqual(signal.SIGALRM, fired[1], "Signal handled was not signal.SIGALRM ({})".format(signal.SIGALRM))

        child_proc = mp.Process(target=fire_alarm, daemon=True)
        child_proc.start()
        child_proc.join(1)

        def signal_reset_wrapper(wrapped_func):
            print("resetting signals in PID {} before calling {}".format(os.getpid(), wrapped_func))
            signal.signal(signal.SIGALRM, signal.SIG_DFL)  # reset first
            wrapped_func()

        child_proc_with_cleared_signals = mp.Process(target=signal_reset_wrapper, args=(fire_alarm,), daemon=True)
        child_proc_with_cleared_signals.start()
        child_proc.join(1)

        fire_alarm()  # prove that clearing in one child process, left the handler intact in the parent process

    def test_terminated_job_triggers_exit_signal_handling_with_retry(self):
        """The child worker process is terminated, and exits indicating the exit signal of the termination. The
        parent monitors this, and initiates exit-handling. Because the dispatcher allows retries, this message
        should be made receivable again on the queue.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                       long_poll_seconds=0, monitor_sleep_time=0.05)
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator",
            target=self._worker_terminator,
            args=(tq, 0.05, logger),
            daemon=True
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(self._work_to_be_terminated, additional_job_args=(tq, wq))
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get_nowait()
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_terminated_job_triggers_exit_signal_handling_to_dlq(self):
        """The child worker process is terminated, and exits indicating the exit signal of the termination. The
        parent monitors this, and initiates exit-handling. Because the dispatcher does not allow retries, the
        message is copied to teh dead letter queue, and deleted from the queue.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                       long_poll_seconds=0, monitor_sleep_time=0.05)

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator",
            target=self._worker_terminator,
            args=(tq, 0.05, logger),
            daemon=True
        )
        terminator.start()  # start terminator

        with self.assertRaises(QueueWorkerProcessError) as err_ctx:
            # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
            # Passing its PID on this Queue will let the terminator know the worker to terminate
            dispatcher.dispatch(self._work_to_be_terminated, additional_job_args=(tq, wq))

        # Ensure to wait on the processes to end with join
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.
        self.assertTrue("SIGTERM" in str(err_ctx.exception), "QueueWorkerProcessError did not mention 'SIGTERM'.")

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message SHOULD have been deleted from the queue
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 0, "Should be NO messages received from queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get_nowait()
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
            # TODO: Test that the "dead letter queue" has this message - maybe by asserting the log statement
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_terminated_parent_dispatcher_exits_with_negative_signal_code(self):
        """After a parent dispatcher process receives an exit signal, and kills its child worker process, it itself
        exits. Verify that the exit code it exits with is the negative value of the signal received, consistent with
        how Python handles this.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                       long_poll_seconds=0, monitor_sleep_time=0.05)
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        dispatch_kwargs = {"job": self._work_to_be_terminated, "additional_job_args": (tq, wq)}
        parent_dispatcher = mp.Process(target=dispatcher.dispatch, kwargs=dispatch_kwargs)
        parent_dispatcher.start()

        # block until worker is running and ready to be terminated, or fail in 3 seconds
        work_done = wq.get(True, 3)
        worker_pid = tq.get(True, 1)
        worker = ps.Process(worker_pid)

        self.assertEqual(msg_body, work_done)
        os.kill(parent_dispatcher.pid, signal.SIGTERM)

        # simulate join(2). Wait at most 2 sec for work to complete
        ps.wait_procs([worker], timeout=2)
        parent_dispatcher.join(1)  # ensure dispatcher completes within 3 seconds. Don't let it run away.

        try:
            # Parent dispatcher process should have an exit code less than zero
            self.assertLess(parent_dispatcher.exitcode, 0)
            self.assertEqual(parent_dispatcher.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            self.assertEqual(msg_body, work_done,
                             "Was expecting to find worker task_id (msg_body) tracked in the queue")
        finally:
            if worker and worker.is_running():
                logger.warning("Dispatched worker process with PID {} did not complete in timeout. "
                               "Killing it.".format(worker.pid))
                os.kill(worker.pid, signal.SIGKILL)
                self.fail("Worker did not complete in timeout as expected. Test fails.")
            self._fail_runaway_processes(logger, dispatcher=parent_dispatcher)

    def test_exit_handler_can_receive_queue_message_as_arg(self):
        """Verify that exit_handlers provided whose signatures allow keyword args can receive the queue message
        as a keyword arg"""
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                       long_poll_seconds=0, monitor_sleep_time=0.05)
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        def exit_handler_with_msg(task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue,  # noqa
                                  queue_message=None):  # noqa
            logger.debug("CLEANUP: performing cleanup with "
                         "exit_handler_with_msg({}, {}, {}".format(task_id, termination_queue, queue_message))
            logger.debug("msg.body = {}".format(queue_message.body))
            work_tracking_queue.put("exit_handling:{}".format(queue_message.body))  # track what we handled

        termination_queue = mp.Queue()
        work_tracking_queue = mp.Queue()
        terminator = mp.Process(
            name="worker_terminator",
            target=self._worker_terminator,
            args=(termination_queue, 0.05, logger),
            daemon=True
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(self._work_to_be_terminated,
                            additional_job_args=(termination_queue, work_tracking_queue),
                            exit_handler=exit_handler_with_msg)
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message SHOULD have been deleted from the queue
            msgs = queue.receive_messages(WaitTimeSeconds=0)

            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item. FIFO queue, so it should be "first out"
            work = work_tracking_queue.get_nowait()
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
            # If the exit_handler was called, and if it passed along args from the original worker, then it should
            # have put the message body into the "work_tracker_queue" after work was started and exit occurred
            exit_handling_work = work_tracking_queue.get_nowait()
            self.assertEqual("exit_handling:{}".format(msg_body), exit_handling_work,
                             "Was expecting to find tracking in the work_tracking_queue of the message being handled "
                             "by the exit_handler")
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    def test_default_to_queue_long_poll_works(self):
        """Same as testing exit handling when allowing retries, but not setting the long_poll_seoconds value,
        to leave it to the default setting.
        """
        logger = logging.getLogger(__name__ + "." + inspect.stack()[0][3])
        logger.setLevel(logging.DEBUG)

        msg_body = randint(1111, 9998)
        queue = sqs_queue()
        queue.send_message(MessageBody=msg_body)

        dispatcher = SQSWorkDispatcher(queue, worker_process_name="Test Worker Process",
                                       monitor_sleep_time=0.05)
        dispatcher.sqs_queue_instance.max_receive_count = 2  # allow retries

        wq = mp.Queue()  # work tracking queue
        tq = mp.Queue()  # termination queue

        terminator = mp.Process(
            name="worker_terminator",
            target=self._worker_terminator,
            args=(tq, 0.05, logger),
            daemon=True
        )
        terminator.start()  # start terminator
        # Start dispatcher with work, and with the inter-process Queue so it can pass along its PID
        # Passing its PID on this Queue will let the terminator know the worker to terminate
        dispatcher.dispatch(self._work_to_be_terminated, additional_job_args=(tq, wq))
        dispatcher._worker_process.join(2)  # wait at most 2 sec for the work to complete
        terminator.join(1)  # ensure terminator completes within 3 seconds. Don't let it run away.

        try:
            # Worker process should have an exitcode less than zero
            self.assertLess(dispatcher._worker_process.exitcode, 0)
            self.assertEqual(dispatcher._worker_process.exitcode, -signal.SIGTERM)
            # Message should NOT have been deleted from the queue, but available for receive again
            msgs = queue.receive_messages(WaitTimeSeconds=0)
            self.assertIsNotNone(msgs)
            self.assertTrue(len(msgs) == 1, "Should be only 1 message received from queue")
            self.assertEqual(msg_body, msgs[0].body, "Should be the same message available for retry on the queue")
            # If the job function was called, it should have put the task_id (msg_body in this case) into the
            # work_tracking_queue as its first queued item
            work = wq.get_nowait()
            self.assertEqual(msg_body, work, "Was expecting to find worker task_id (msg_body) tracked in the queue")
        finally:
            self._fail_runaway_processes(logger, worker=dispatcher._worker_process, terminator=terminator)

    @classmethod
    def _worker_terminator(cls, terminate_queue: mp.Queue, sleep_interval=0, logger=logging.getLogger(__name__)):
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

    @classmethod
    def _work_to_be_terminated(cls, task_id, termination_queue: mp.Queue, work_tracking_queue: mp.Queue,
                               *args, **kwargs):
        # Put task_id in the work_tracking_queue so we know we saw this work
        work_tracking_queue.put(task_id)
        # Put PID of worker process in the termination_queue to let worker_terminator proc know what to kill
        termination_queue.put(os.getpid())
        sleep(0.25)  # hang for a short period to ensure terminator has time to kill this

    def _fail_runaway_processes(self,
                                logger,
                                worker: mp.Process = None,
                                terminator: mp.Process = None,
                                dispatcher: mp.Process = None):
        fail_with_runaway_proc = False
        if worker and worker.is_alive():
            logger.warning("Dispatched worker process with PID {} did not complete in timeout. "
                           "Killing it.".format(worker.pid))
            os.kill(worker.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if terminator and terminator.is_alive():
            logger.warning("Terminator worker process with PID {} did not complete in timeout. "
                           "Killing it.".format(terminator.pid))
            os.kill(terminator.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if dispatcher and dispatcher.is_alive():
            logger.warning("Parent dispatcher process with PID {} did not complete in timeout. "
                           "Killing it.".format(dispatcher.pid))
            os.kill(dispatcher.pid, signal.SIGKILL)
            fail_with_runaway_proc = True
        if fail_with_runaway_proc:
            self.fail("Worker or its Terminator or the Dispatcher did not complete in timeout as expected. Test fails.")
