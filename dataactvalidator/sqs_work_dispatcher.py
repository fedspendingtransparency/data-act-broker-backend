import logging
import inspect
import json
import os

import psutil as ps
import signal
import sys
import time
import multiprocessing as mp

from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError, NoRegionError

from dataactcore.aws.sqsHandler import sqs_queue
from dataactvalidator.validator_logging import log_job_message

# Not a complete list of signals
# Only the following are allowed to be used with signal() on Windows
#    SIGABRT, SIGFPE, SIGILL, SIGINT, SIGSEGV, SIGTERM, or SIGBREAK (windows only)
# NOTE: 1-15 are relatively standard on unix platforms; > 15 can change from platform to platform
BSD_SIGNALS = {
        1: "SIGHUP [1] (Hangup detected on controlling terminal or death of controlling process)",
        2: "SIGINT [2] (Interrupt from keyboard)",
        3: "SIGQUIT [3] (Quit from keyboard)",
        4: "SIGILL [4] (Illegal Instruction)",
        6: "SIGABRT [6] (Abort signal from abort(3))",
        8: "SIGFPE [8] (Floating point exception)",
        9: "SIGKILL [9] (Non-catchable, non-ignorable kill)",
        11: "SIGSEGV [11] (Invalid memory reference)",
        14: "SIGALRM [14] (Timer signal from alarm(2))",
        15: "SIGTERM [15] (Software termination signal)",
        19: "SIGSTOP [19] (Suspend process execution)",  # NOTE: 17 on Mac OSX, 19 on RHEL
        20: "SIGTSTP [20] (Interrupt from keyboard to suspend (CTRL-Z)",  # NOTE: 18 on Mac OSX, 20 on RHEL
    }
if sys.platform == "win32":
    BSD_SIGNALS[0] = "CTRL_C_EVENT [0] (Quit detected on controlling terminal with CTRL-C)"
    BSD_SIGNALS[1] = "CTRL_BREAK_EVENT [1] (Quit detected on controlling terminal with CTRL-BREAK)"
    BSD_SIGNALS[21] = "SIGBREAK [21] (Quit detected on controlling terminal with CTRL-BREAK)"


class SQSWorkDispatcher:
    _logger = logging.getLogger(__name__)

    # Delineate each of the signals we want to handle, which represent a case where the work being performed
    # is prematurely halting, and the process will exit
    EXIT_SIGNALS = []
    if sys.platform == "win32":
        EXIT_SIGNALS = [signal.SIGABRT, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM,
                        signal.SIGBREAK, signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT]
    else:
        EXIT_SIGNALS = [signal.SIGHUP, signal.SIGABRT, signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]
        # NOTE: We are not handling signal.SIGSTOP or signal.SIGTSTP because those are suspensions.
        # There may be valid cases to suspend and then later resume the process.
        # It is even done here via multiprocessing.Process.suspend() to suspend during cleanup.
        # So we do NOT want to handle that

    def __init__(
            self,
            sqs_queue_instance,
            worker_process_name=None,
            default_visibility_timeout=60,
            long_poll_seconds=None,
            monitor_sleep_time=5,
            exit_handling_timeout=30):
        """
        SQSWorkDispatcher object that is used to pull work from an SQS queue, and then dispatch it to be
        executed on a child worker process.

        This has the benefit of the worker process terminating after completion of the work, which will clear all
        resources used during its execution, and return memory to the operating system. It also allows for periodically
        monitoring the progress of the worker process from the parent process, and extending the SQS
        VisibilityTimeout, so that the message is not prematurely terminated and/or moved to the Dead Letter Queue

        :param sqs_queue_instance: the SQS queue to get work from
        :param worker_process_name: the name to give to the worker process. It will use the name of the callable job
               to be executed if not provided
        :param default_visibility_timeout: how long until the message is made visible in the queue again,
               for other consumers. If it only allows 1 retry, this may end up directing it to the Dead Letter queue
               when the next consumer attempts to receive it.
        :param long_poll_seconds: if it should wait patiently for some time to receive a message when requesting.
               Defaults to None, which means it should honor the value configured on the SQS queue
        :param monitor_sleep_time: periodicity to check up on the status of the worker process
        :param exit_handling_timeout: expected window of time during which cleanup should complete (not guaranteed).
               This for example would be the time to finish cleanup before the messages is re-queued or DLQ'd
        """
        self.sqs_queue_instance = sqs_queue_instance
        self.worker_process_name = worker_process_name
        self._default_visibility_timeout = default_visibility_timeout
        self._long_poll_seconds = long_poll_seconds
        self._monitor_sleep_time = monitor_sleep_time
        self._exit_handling_timeout = exit_handling_timeout
        self._current_sqs_message = None
        self._worker_process = None
        self._job_args = ()
        self._exit_handler = None
        self._parent_dispatcher_pid = os.getppid()
        self._sqs_heartbeat_log_period_seconds = 15  # log the heartbeat extension of visibility at most this often

        # Flag used to prevent handling an exit signal more than once, if multiple signals come in succession
        # True when the parent dispatcher process is handling one of the signals in EXIT_SIGNALS, otherwise False
        self._handling_exit_signal = False
        # Flagged True when exit handling should lead to an exit of the parent dispatcher process
        self._dispatcher_exiting = False

        if self._monitor_sleep_time >= self._default_visibility_timeout:
            msg = "_monitor_sleep_time must be less than _default_visibility_timeout. " \
                  "Otherwise job duplication can occur"
            raise QueueWorkDispatcherError(msg)

        # Map handler functions for each of the exit signals we want to handle on the parent dispatcher process
        for sig in self.EXIT_SIGNALS:
            signal.signal(sig, self._handle_exit_signal)

    @property
    def is_exiting(self):
        """True when this parent dispatcher process has received a signal that will lead to the process exiting"""
        return self._dispatcher_exiting

    @property
    def allow_retries(self):
        """Determine from the provided queue if retries of messages should be performed

        If False, the message will not be returned to the queue for retries by other consumers
        """
        redrive_policy = self.sqs_queue_instance.attributes.get("RedrivePolicy")
        if not redrive_policy:
            return True  # Messages in queues without a redrive policy basically have endless retries
        redrive_json = json.loads(redrive_policy)
        retries = redrive_json.get("maxReceiveCount")
        return retries and retries > 1

    def _dispatch(self, job, job_args, worker_process_name=None, exit_handler=None):
        """
        Dispatch work to be performed in a newly started worker process

        Execute the callable job that will run in a separate child worker process. The worker process
        will be monitored so that heartbeats can be sent to SQS to allow it to keep going.
        :param job: callable to use as the target of a the new child process
        :param tuple job_args: arguments to be passed to the job
        :param worker_process_name: Name given to the newly created child process. If not already set, defaults to
               the name of the provided job callable
        :param callable exit_handler: a callable to be called when handling an `EXIT_SIGNAL` signal, giving the
               opportunity to perform cleanup before the process exits. Gets the job_args passed to it when run
        :return: True if a message was found on the queue and dispatched, otherwise False if nothing on the queue
        """
        self.worker_process_name = worker_process_name or self.worker_process_name or job.__name__
        log_job_message(
            logger=self._logger,
            message="Creating and starting worker process named [{}] to invoke callable [{}] "
                    "with arguments [{}]".format(self.worker_process_name, job, job_args)
        )

        # Set the exit_handler function if provided, or reset to None
        # Also save the job_args, which will be passed to the exit_handler
        self._exit_handler = exit_handler
        self._job_args = job_args

        # Use the 'fork' method to create a new child process.
        # This shares the same python interpreter and memory space and references as the parent process
        # A side-effect of that is that it inherits the signal-handlers of teh parent process. So wrap the job to be
        # executed with some before-advice that resets the signal-handlers to python-defaults within the child process
        ctx = mp.get_context("fork")

        def signal_reset_wrapper(func, args):
            # Reset signal handling to defaults in process that calls this
            for sig in self.EXIT_SIGNALS:
                signal.signal(sig, signal.SIG_DFL)
            # Then execute the given function with the given args
            func(*args)

        self._worker_process = ctx.Process(
            name=self.worker_process_name,
            target=signal_reset_wrapper,
            args=(job, job_args),
            daemon=True  # daemon=True ensures that if the parent dispatcher dies, it will kill this child worker
        )
        self._worker_process.start()
        log_job_message(
            logger=self._logger,
            message="Worker process named [{}] started with process ID [{}]".format(
                self.worker_process_name,
                self._worker_process.pid
            ),
            is_debug=True
        )
        self._monitor_work_progress()
        return True

    def dispatch(self, job, message_transformer=lambda x: x.body, additional_job_args=(),
                 worker_process_name=None, exit_handler=None):
        """
        Get work from the queue and dispatch it in a newly starter worker process

        Poll the queue for a message to work on. Combine that message with any
        _additional_job_args passed into the constructor, and feed this complete set of arguments to the provided
        callable job that will be executed in a separate worker process. The worker process will be monitored so that
        heartbeats can be sent to the SQS to allow it to keep going.

        :param job: the callable to execute as work for the job
        :param message_transformer: A lambda to extract arguments to be passed to the callable from the queue message.
               By default, a function that just returns the message body is used.
        :param additional_job_args: Additional arguments to provide to the callable, along with those from the message
        :param worker_process_name: Name given to the newly created child process. If not already set, defaults to
               the name of the provided job callable
        :param exit_handler: a callable to be called when handling an `EXIT_SIGNAL` signal, giving the
               opportunity to perform cleanup before the process exits. Gets the job_args passed to it when run
        :raises SystemExit(1): If it can't connect to the queue or receive messages
        :raises QueueWorkerProcessError: Under various conditions where the child worker process fails to run the job
        :raises QueueWorkDispatcherError: Under various conditions where the parent dispatcher process can't
                orchestrate work execution, monitoring, or exit-signal-handling
        :return: True if a message was found on the queue and dispatched, otherwise False if nothing on the queue
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False
        msg_args = message_transformer(self._current_sqs_message)
        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(i for i in msg_args) + additional_job_args
        else:
            job_args = (msg_args,) + additional_job_args
        return self._dispatch(job, job_args, worker_process_name, exit_handler)

    def dispatch_by_message_attribute(self, message_transformer, additional_job_args=(),
                                      worker_process_name=None):
        """
        Use a provided function to derive the callable job and its arguments from attributes within the queue message

        :param message_transformer: The callable function that returns a tuple of (job, job_args) or (job,
               job_args, exit_handler) if an exit_handler is required
        :param additional_job_args: Additional arguments to provide to the callable, along with those from the message
        :param worker_process_name: Name given to the newly created child process. If not already set, defaults to
               the name of the provided job callable
        :raises SystemExit(1): If it can't connect to the queue or receive messages
        :raises QueueWorkerProcessError: Under various conditions where the child worker process fails to run the job
        :raises QueueWorkDispatcherError: Under various conditions where the parent dispatcher process can't
                orchestrate work execution, monitoring, or exit-signal-handling
        :return: True if a message was found on the queue and dispatched, otherwise False if nothing on the queue
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False
        results = message_transformer(self._current_sqs_message)

        # exit_handler: a callable to be called when handling an `EXIT_SIGNAL` signal, giving the
        # opportunity to perform cleanup before the process exits. Gets the job_args passed to it when run
        # If provided in this method, it needs to be provided as an optional 3rd item of the
        # message_transformer's returned tuple
        exit_handler = None if not len(results) == 3 else results[2]  # assign 3rd return val as optional exit_handler
        job, msg_args = results[:2]
        log_job_message(
            logger=self._logger,
            message="Got job [{}] and msg_args [{}]. exit_handler = [{}]".format(job, msg_args, exit_handler),
            is_debug=True
        )
        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(i for i in msg_args) + additional_job_args
        else:
            job_args = (msg_args,) + additional_job_args
        return self._dispatch(job, job_args, worker_process_name, exit_handler)

    def _dequeue_message(self, wait_time):
        """
        Attempt to get a message from the queue.
        :param wait_time: If no message is readily available, wait for this many seconds for one to arrive before
               returning
        :return: 1 message from the queue if there are messages, otherwise return None
        """
        try:
            received_messages = self.sqs_queue_instance.receive_messages(
                WaitTimeSeconds=wait_time,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                VisibilityTimeout=self._default_visibility_timeout,
                MaxNumberOfMessages=1,
            )
        except (EndpointConnectionError, ClientError, NoCredentialsError, NoRegionError) as conn_exc:
            log_job_message(logger=self._logger, message="SQS connection issue. See Traceback and investigate settings",
                            is_exception=True)
            raise SystemExit(1) from conn_exc
        except Exception as exc:
            log_job_message(logger=self._logger, message="Unknown error occurred when attempting to receive messages "
                                                         "from the queue. See Traceback",
                            is_exception=True)
            raise SystemExit(1) from exc

        if received_messages:
            self._current_sqs_message = received_messages[0]
            log_job_message(logger=self._logger, message="Message received: {}".format(self._current_sqs_message.body))

    def delete_message_from_queue(self):
        if self._current_sqs_message is None:
            log_job_message(
                logger=self._logger,
                message="Message to delete does not exist. "
                        "Message might have previously been moved, released, or deleted",
                is_warning=True
            )
            return
        try:
            self._current_sqs_message.delete()
            self._current_sqs_message = None
        except Exception as exc:
            log_job_message(
                logger=self._logger,
                message="Unable to delete SQS message from queue. "
                        "Message might have previously been deleted upon completion or failure",
                is_exception=True
            )
            raise QueueWorkDispatcherError() from exc

    def surrender_message_to_other_consumers(self, delay=0):
        """
        Return the message back into its original queue for other consumers to process it.

        Does this by making it immediately visible (it was always there, just invisible).
        See also: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/
                  sqs-visibility-timeout.html#terminating-message-visibility-timeout

        :param delay: How long to wait before message becomes visible to other consumers. Default is 0 seconds (
               immediately consumable)
        """
        self._set_message_visibility(delay)

    def move_message_to_dead_letter_queue(self):
        """Move the current message to the Dead Letter Queue.

        Moves the message to the Dead Letter Queue associated with this worker's SQS queue via its RedrivePolicy.
        Then delete the message from its originating queue.

        :raises
        :raises QueueWorkDispatcherError: Raise an error if there is no RedrivePolicy or a Dead Letter Queue is not
                configured, specified, or connectible for this queue.

        """
        if self._current_sqs_message is None:
            log_job_message(
                logger=self._logger,
                message="Unable to move SQS message to the dead letter queue. No current message exists. "
                        "Message might have previously been moved, released, or deleted",
                is_warning=True
            )
            return

        dlq_name = "not discovered yet"
        try:
            redrive_policy = self.sqs_queue_instance.attributes.get("RedrivePolicy")
            if not redrive_policy:
                error = "Failed to move message to dead letter queue. Cannot get RedrivePolicy for SQS queue \"{}\". " \
                        "It was not set, or was not included as an attribute to be " \
                        "retrieved with this queue.".format(self.sqs_queue_instance)
                log_job_message(logger=self._logger, message=error, is_error=True)
                raise QueueWorkDispatcherError(error)

            redrive_json = json.loads(redrive_policy)
            dlq_arn = redrive_json.get("deadLetterTargetArn")
            if not dlq_arn:
                error = "Failed to move message to dead letter queue. " \
                        "Cannot find a dead letter queue in the RedrivePolicy " \
                        "for SQS queue \"{}\"".format(self.sqs_queue_instance)
                log_job_message(logger=self._logger, message=error, is_error=True)
                raise QueueWorkDispatcherError(error)
            dlq_name = dlq_arn.split(':')[-1]

            # Copy the message to the designated dead letter queue
            dlq = sqs_queue(queue_name=dlq_name)
            message_attr = self._current_sqs_message.message_attributes.copy() if \
                self._current_sqs_message.message_attributes else {}
            dlq_response = dlq.send_message(
                MessageBody=self._current_sqs_message.body,
                MessageAttributes=message_attr
            )
        except Exception as exc:
            error = "Error occurred when parent dispatcher with PID [{}] tried to move the message " \
                    "for worker process with PID [{}] " \
                    "to the dead letter queue [{}].".format(os.getpid(), self._worker_process.pid, dlq_name)
            log_job_message(logger=self._logger, message=error, is_exception=True)
            raise QueueWorkDispatcherError(error) from exc

        log_job_message(
            logger=self._logger,
            message="Message sent to dead letter queue \"{}\" "
                    "with [{}] response code. "
                    "Now deleting message from origin queue".format(dlq_name,
                                                                    dlq_response['ResponseMetadata']['HTTPStatusCode']),
            is_debug=True
        )
        self.delete_message_from_queue()

    def _monitor_work_progress(self):
        monitor_process = True
        heartbeats = 0
        while monitor_process:
            monitor_process = False
            if self._worker_process.is_alive():
                # Process still working. Send "heartbeat" to SQS so it may continue
                if (heartbeats * self._monitor_sleep_time) >= self._sqs_heartbeat_log_period_seconds:
                    log_job_message(
                        logger=self._logger,
                        message="Job worker process with PID [{}] is still running. "
                                "Renewing VisibilityTimeout of {} seconds".format(self._worker_process.pid,
                                                                                  self._default_visibility_timeout),
                        is_debug=True
                    )
                    heartbeats = 0
                else:
                    heartbeats += 1
                self._set_message_visibility(self._default_visibility_timeout)
                time.sleep(self._monitor_sleep_time)
                monitor_process = True
            elif self._worker_process.exitcode == 0:
                # If process exits with 0: success! Remove from queue
                log_job_message(
                    logger=self._logger,
                    message="Job worker process with PID [{}] completed with 0 for exit code (success). Deleting "
                            "message from the queue".format(self._worker_process.pid)
                )
                self.delete_message_from_queue()
            elif self._worker_process.exitcode > 0:
                # If process exits with positive code, an ERROR occurred within the worker process.
                # Don't delete the message, don't force retry, and don't force into dead letter queue.
                # Let the VisibilityTimeout expire, and the queue will handle whether to retry or put in
                # the DLQ based on its queue configuration.
                # Raise an exception to give control back to the caller to allow any error-handling to take place there
                message = "Job worker process with PID [{}] errored with exit code: {}.".format(
                    self._worker_process.pid,
                    self._worker_process.exitcode
                )
                log_job_message(logger=self._logger, message=message, is_error=True)
                raise QueueWorkerProcessError(message)
            elif self._worker_process.exitcode < 0:
                # If process exits with a negative code, process was terminated by a signal since
                # a Python subprocess returns the negative value of the signal.
                signum = self._worker_process.exitcode * -1
                # In the rare case where the child worker process's exit signal is detected before its parent
                # process received the same signal, proceed with handling the child's exit signal
                self._handle_exit_signal(signum=signum, frame=None, parent_dispatcher_signaled=False)

    def _handle_exit_signal(self, signum, frame, parent_dispatcher_signaled=True, is_retry=False):
        """
        Attempt to gracefully handle the exiting of the job as a result of receiving an exit signal.

        NOTE: This handler is only expected to be run from the parent dispatcher process. It is an error
        condition if it is invoked from the child worker process. Signals received in the child worker
        process should not have this handler registered for those signals. Because signal handlers registered for a
        parent process are inherited by forked child processes (see: "man 7 signal" docs and search for "inherited"),
        this handler will initially be registered for each of this class's EXIt_SIGNALS; however, those handlers
        are reset to their default as the worker process is started by a wrapper function around the job to execute.

        The signal very likely indicates a non-error failure scenario from which the job might be restarted or rerun,
        if allowed. Handle cleanup, logging, job retry logic, etc.
        NOTE: Order is important:
            1. "Lock" (guard) this exit handler, to handle one exit signal only
            2. Extend VisibilityTimeout by _exit_handling_timeout, to allow time for cleanup
            3. Suspend child worker process, if alive, else skip (to not interfere with cleanup)
            4. Execute pre-exit cleanup
            5. Move the message (back to the queue, or to the dead letter queue)
            6. Kill child worker process
            7. Exit this parent dispatcher process if it received an exit signal

        :param signum: number representing the signal received
        :param frame: Frame passed in with the signal
        :param parent_dispatcher_signaled: Assumed that this handler is being called as a result of signaling that
               happened on the parent dispatcher process. If this was instead the parent detecting that the child
               exited due to a signal, set this to False. Defaults to True.
        :param is_retry: If this is the 2nd and last try to handled the signal and perform pre-exit cleanup
        :raises QueueWorkerProcessError: When the _exit_handling function cannot be completed in time to perform
                pre-exit cleanup
        :raises QueueWorkerProcessError: If only the child process died (not the parent dispatcher), but the message it
                was working was moved to the Dead Letter Queue because the queue is not configured to allow retries,
                raise an exception so the parent process might mark the job as failed with its normal error-handling
                process
        :raises SystemExit: If the parent dispatcher process received an exit signal, this should be raised
                after handling the signal, as part of running os.kill of that process using the original signal number
                that was originally handled
        :return: None
        """
        # If the process handling this signal is the same as the started worker process, this *is* the worker process
        is_worker_process = self._worker_process and self._worker_process.pid == os.getpid()

        # If the signal is in BSD_SIGNALS, use the human-readable string, otherwise use the signal value
        signal_or_human = BSD_SIGNALS.get(signum, signum)

        if is_worker_process:
            log_job_message(
                logger=self._logger,
                message="Worker process with PID [{}] received signal [{}] while running. "
                        "It should not be handled in this exit signal handler, "
                        "as it is meant to run from the parent dispatcher process."
                        "Exiting worker process with original exit signal.".format(os.getpid(), signal_or_human),
                is_warning=True
            )

            # Ensure the worker exits as would have occurred if not handling its signals
            signal.signal(signum, signal.SIG_DFL)
            os.kill(self._worker_process.pid, signum)
        elif not parent_dispatcher_signaled:
            log_job_message(
                logger=self._logger,
                message="Job worker process with PID [{}] exited due to signal with exit code: [{}]. "
                        "Gracefully handling exit of job".format(
                            self._worker_process.pid,
                            self._worker_process.exitcode
                        ),
                is_error=True
            )

        else:  # dispatcher signaled
            log_job_message(
                logger=self._logger,
                message="Parent dispatcher process with PID [{}] received signal [{}] while running. "
                        "Gracefully stopping job being worked".format(os.getpid(), signal_or_human),
                is_error=True
            )

            # Make sure the parent dispatcher process exits after handling the signal by setting this flag
            self._dispatcher_exiting = True

        if self._handling_exit_signal:
            # Don't allow the parent dispatcher process to handle more than one exit signal
            return

        self._handling_exit_signal = True

        try:
            # Extend message visibility for as long is given for the exit handling to process, so message does not get
            # prematurely returned to the queue or moved to the dead letter queue.
            # Give it a 5 second buffer in case retry mechanics put it over the timeout
            self._set_message_visibility(self._exit_handling_timeout + 5)

            if self._worker_process.is_alive():
                worker = ps.Process(self._worker_process.pid)
                if not is_retry:
                    # Suspend the child worker process so it does not conflict with doing cleanup in the exit_handler
                    worker.suspend()
                else:
                    # This is try 2. The cleanup was not able to complete on try 1 with the worker process merely
                    # suspended. There could be some kind of transaction deadlock. Kill the worker to clear the way
                    # for cleanup retry
                    worker.kill()

            if self._exit_handler is not None:
                # Execute cleanup procedures to handle exiting of the worker process
                # Wrap cleanup in a fixed timeout, and a retry (one time)
                try:
                    with ExecutionTimeout(self._exit_handling_timeout):
                        # Call _exit_handler callable to do cleanup
                        queue_message_param = inspect.signature(self._exit_handler).parameters.get("queue_message")
                        arg_spec = inspect.getfullargspec(self._exit_handler)
                        if queue_message_param or arg_spec.varkw == "kwargs":
                            # it defines a param named "queue_message", or accepts kwargs,
                            # so pass along the message as "queue_message" in  case it's needed
                            self._exit_handler(*self._job_args, queue_message=self._current_sqs_message)
                        else:
                            self._exit_handler(*self._job_args)
                except TimeoutError:
                    if not is_retry:
                        log_job_message(
                            logger=self._logger,
                            message="Could not perform cleanup during exiting of job in allotted "
                                    "_exit_handling_timeout ({}s). "
                                    "Retrying once.".format(self._exit_handling_timeout),
                            is_warning=True
                        )
                        self._handle_exit_signal(signum, frame, is_retry=True)  # attempt retry
                    else:
                        message = "Could not perform cleanup during exiting of job in allotted " \
                                    "_exit_handling_timeout ({}s) after 2 tries. " \
                                    "Raising exception.".format(self._exit_handling_timeout)
                        log_job_message(
                            logger=self._logger,
                            message=message,
                            is_error=True
                        )
                        raise QueueWorkerProcessError()

            if self.allow_retries:
                log_job_message(
                    logger=self._logger,
                    message="Dispatcher for this job allows retries of the message. "
                            "Attempting to return message to its queue immediately."
                )
                self.surrender_message_to_other_consumers()
            else:
                # Otherwise, attempt to send message directly to the dead letter queue
                log_job_message(
                    logger=self._logger,
                    message="Dispatcher for this job does not allow message to be retried. "
                            "Attempting to move message to its Dead Letter Queue."
                )
                self.move_message_to_dead_letter_queue()

            # Now that SQS message has been handled, ensure worker process is dead
            if self._worker_process.is_alive():
                worker = ps.Process(self._worker_process.pid)
                # Use kill instead of multiprocess.Process.terminate() or psutil.Process.terminate(), since each of
                # those send a signal.SIGTERM which is not as immediate and final as kill (signal.SIGKILL)
                log_job_message(
                    logger=self._logger,
                    message="Message handled. Now killing worker process with PID [{}] "
                            "as it is alive but has no more work to do.".format(worker.pid),
                    is_debug=True
                )
                worker.kill()
        finally:
            self._handling_exit_signal = False
            if self._dispatcher_exiting:
                # An exit signal was received by the parent dispatcher process.
                # Continue with exiting the parent process, as per the original signal, after having handled it
                log_job_message(
                    logger=self._logger,
                    message="Exiting from parent dispatcher process with PID [{}] "
                            "to culminate exit-handling of signal [{}]".format(os.getpid(), signal_or_human),
                    is_debug=True
                )
                # Simply doing sys.exit or raise SystemExit, even with a given negative exit code won't exit this
                # process with the negative signal value, as is the Python custom.
                # So override the assigned handler for the signal we're handing, resetting it back to default,
                # and kill the process using that signal to get it to exit as it would if we never handled it
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)
            elif not self.allow_retries:
                # If only the child process died, but the message it was working was moved to the Dead Letter Queue
                # because the queue is not configured to allow retries, raise an exception so the parent process
                # might mark the job as failed with its normal error-handling process
                raise QueueWorkerProcessError("Worker process with PID [{}] was not able to complete its job due to "
                                              "interruption from exit signal [{}]. The queue message for that job "
                                              "was moved to the Dead Letter Queue, where it can be reviewed for a "
                                              "manual restart, or other remediation.".format(self._worker_process.pid,
                                                                                             signal_or_human))


    def _set_message_visibility(self, new_visibility):
        if self._current_sqs_message is None:
            log_job_message(
                logger=self._logger,
                message="No SQS message to change visibility of. Message might have previously been released, "
                        "deleted, or moved to dead letter queue.",
                is_warning=True
            )
            return
        try:
            self._current_sqs_message.change_visibility(VisibilityTimeout=new_visibility)
        except Exception as exc:
            message = "Unable to set VisibilityTimeout. " \
                      "Message might have previously been deleted upon completion or failure"
            log_job_message(logger=self._logger, message=message, is_exception=True)
            raise QueueWorkDispatcherError(message) from exc


class QueueWorkerProcessError(Exception):
    """Custom exception representing the scenario where the spawned worker process has failed
     with a non-zero exit code, indicating some kind of failure.
    """
    pass


class QueueWorkDispatcherError(Exception):
    """Custom exception representing the scenario where the parent process dispatching to and monitoring the worker
     process has failed with some kind of unexpected exception.
    """
    pass


class ExecutionTimeout:
    def __init__(self, seconds=0, error_message='Execution took longer than the allotted time'):
        self.seconds = seconds
        self.error_message = error_message

    def _timeout_handler(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        if self.seconds > 0:
            signal.signal(signal.SIGALRM, self._timeout_handler)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
