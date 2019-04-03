import logging
import json
import signal
import time

from multiprocessing import Process
from botocore.exceptions import ClientError

from dataactcore.aws.sqsHandler import sqs_queue
from dataactvalidator.validator_logging import log_job_message


BSD_SIGNALS = {
        1: "SIGHUP [1] (Hangup detected on controlling terminal or death of controlling process)",
        2: "SIGINT [2] (Interrupt from keyboard)",
        3: "SIGQUIT [3] (Quit from keyboard)",
        4: "SIGILL [4] (Illegal Instruction)",
        6: "SIGABRT [6] (Abort signal from abort(3))",
        8: "SIGFPE [8] (Floating point exception)",
        9: "SIGKILL [9] (non-catchable, non-ignorable kill)",
        11: "SIGSEGV [11] (Invalid memory reference)",
        14: "SIGALRM [12] (Timer signal from alarm(2))",
        15: "SIGTERM [15] (software termination signal)",
    }


class SQSWorkDispatcher:
    _logger = logging.getLogger(__name__)

    def __init__(
            self,
            sqs_queue_instance,
            worker_process_name=None,
            allow_retries=True,
            default_visibility_timeout=60,
            long_poll_seconds=10,
            monitor_sleep_time=5
    ):
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
        :param allow_retries: if False, the message will not be returned to the queue for retries by other consumers
        :param default_visibility_timeout: how long until the message is made visible in the queue again,
               for other consumers. If it only allows 1 retry, this may end up directing it to the Dead Letter queue
               when the next consumer attempts to receive it.
        :param long_poll_seconds: if it should wait patiently for some time to receive a message when requesting.
        :param monitor_sleep_time: periodicity to check up on the status of the worker process
        """
        self.sqs_queue_instance = sqs_queue_instance
        self.worker_process_name = worker_process_name
        self.allow_retries = allow_retries
        self._default_visibility_timeout = default_visibility_timeout
        self._long_poll_seconds = long_poll_seconds
        self._monitor_sleep_time = monitor_sleep_time
        self._current_sqs_message = None
        self._worker_process = None

        if self._monitor_sleep_time >= self._default_visibility_timeout:
            msg = "_monitor_sleep_time must be less than _default_visibility_timeout. " \
                  "Otherwise job duplication can occur"
            raise QueueWorkDispatcherError(msg)

        # Map handler functions for each of the interrupt signals we want to handle on the parent dispatcher process
        for sig in [signal.SIGINT, signal.SIGQUIT, signal.SIGTERM]:
            signal.signal(sig, self._signal_handler)

    def _dispatch(self, job, job_args):
        """
        Dispatch work to be performed in a newly started worker process

        Execute the callable job that will run in a separate child worker process. The worker process
        will be monitored so that heartbeats can be sent to SQS to allow it to keep going.
        """
        name = self.worker_process_name or job.__name__
        log_job_message(
            logger=self._logger,
            message="Creating and starting worker process named [{}] to invoke callable [{}] "
                    "with arguments [{}]".format(name, job, job_args)
        )
        self._worker_process = Process(name=name, target=job, args=job_args)
        self._worker_process.start()
        log_job_message(
            logger=self._logger,
            message="Worker process started with process ID [{}]".format(self._worker_process.pid),
            is_debug=True
        )
        self._monitor_work_progress()
        return True

    def dispatch_with_message(self, job, message_transformer=lambda x: x.body, additional_job_args=()):
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
        :return: None
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False
        msg_args = message_transformer(self._current_sqs_message)
        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(i for i in msg_args) + additional_job_args
        else:
            job_args = (msg_args,) + additional_job_args
        return self._dispatch(job, job_args)

    def dispatch_by_message_attribute(self, message_transformer, additional_job_args=()):
        """
        Use a provided function to derive the callable job and its arguments from attributes within the queue message

        :param message_transformer: The callable function that returns a job and arguments given a queue message
        :param additional_job_args: Additional arguments to provide to the callable, along with those from the message
        :return: None
        """
        self._dequeue_message(self._long_poll_seconds)
        if self._current_sqs_message is None:
            return False
        job, msg_args = message_transformer(self._current_sqs_message)
        log_job_message(
            logger=self._logger,
            message="Got job [{}] and msg_args [{}]".format(job, msg_args),
            is_debug=True
        )
        if isinstance(msg_args, list) or isinstance(msg_args, tuple):
            job_args = tuple(i for i in msg_args) + additional_job_args
        else:
            job_args = (msg_args,) + additional_job_args
        return self._dispatch(job, job_args)

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
                MessageAttributeNames=["All"],
                VisibilityTimeout=self._default_visibility_timeout,
                MaxNumberOfMessages=1,
            )
        except ClientError as exc:
            log_job_message(logger=self._logger, message="SQS connection issue. Investigate settings",
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

    def surrender_message_to_other_consumers(self):
        """Put the message back into its original queue for other consumers to process it.

        Does this by making it immediately visible (it was always there, just invisible).
        See also: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
        """
        self._set_message_visibility(0)

    def move_message_to_dead_letter_queue(self):
        """Move the current message to the Dead Letter Queue.

        Moves the message to the Dead Letter Queue associated with this worker's SQS queue via its RedrivePolicy.
        Then delete the message from its originating queue.

        :raises Exception: Throw an error if there is not RedrivePolicy or a Dead Letter Queue is not configured or
                specified for this queue.
        """
        if self._current_sqs_message is None:
            log_job_message(
                logger=self._logger,
                message="Unable to move SQS message to the dead letter queue. Not current message exists. "
                        "Message might have previously been moved, released, or deleted",
                is_warning=True
            )
            return

        redrive_policy = self.sqs_queue_instance.attributes.get("RedrivePolicy")
        if not redrive_policy:
            raise QueueWorkDispatcherError("Failed to move message to dead letter queue. "
                                           "Cannot get RedrivePolicy for SQS queue \"{}\". "
                                           "It was not set, or was not included as an attribute to "
                                           "be retrieved with this queue.".format(self.sqs_queue_instance))
        redrive_json = json.loads(redrive_policy)
        dlq_arn = redrive_json.get("deadLetterTargetArn")
        if not dlq_arn:
            raise QueueWorkDispatcherError("Failed to move message to dead letter queue. "
                                           "Cannot find a dead letter queue in the "
                                           "RedrivePolicy for SQS queue \"{}\"".format(self.sqs_queue_instance))
        dlq_name = dlq_arn.split(':')[-1]
        # Copy the message to the designated dead letter queue
        dlq = sqs_queue(queue_name=dlq_name)
        dlq.send_message(
            MessageBody=self._current_sqs_message.body,
            MessageAttributes=self._current_sqs_message.message_attributes
        )
        self.delete_message_from_queue()

    def _monitor_work_progress(self):
        monitor_process = True
        while monitor_process:
            monitor_process = False
            log_job_message(
                logger=self._logger,
                message="Checking status of worker process with PID [{}]".format(self._worker_process.pid),
                is_debug=True
            )
            if self._worker_process.is_alive():
                # Process still working. Send "heartbeat" to SQS so it may continue
                log_job_message(
                    logger=self._logger,
                    message="Job worker process with PID [{}] is still running. "
                            "Extending VisibilityTimeout".format(self._worker_process.pid),
                    is_debug=True
                )
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
                message = "Job worker process with PID [{}] errored with exit code: {}.".format(
                    self._worker_process.pid,
                    self._worker_process.exitcode
                )
                log_job_message(logger=self._logger, message=message, is_error=True)
                # TODO: Reason about whether it is right to raise an exception here for the calling code to be aware
                # TODO: that the worker was incomplete with error. What is the result either way:
                # TODO: If no raise: Loop stops looping. Message is never deleted. Vis times out, and message is either
                # TODO: retried or DLQ'd
                # TODO: If raised: Same as above. But if raised exception not handled, it could bubble up and bring
                # TODO: down the parent process too. Do we need it/want it to bring down the parent? All the work and
                # TODO: resources were discarded with the child proc
                raise QueueWorkerProcessError(message)
            elif self._worker_process.exitcode < 0:
                # If process exits with a negative code, process was terminated by a signal since
                # a Python subprocess returns the negative value of the signal.
                signum = self._worker_process.exitcode * -1
                try:
                    self._halt_job_with_signal(signum, True)
                finally:
                    time.sleep(self._monitor_sleep_time)  # Wait. System might be shutting down.

    def _set_message_visibility(self, new_visibility):
        if self._current_sqs_message is None:
            log_job_message(
                logger=self._logger,
                message="No SQS message to change visibility of. Message might have previously been released or "
                        "deleted",
                is_warning=True
            )
            return
        try:
            self._current_sqs_message.change_visibility(VisibilityTimeout=new_visibility)
        except ClientError as exc:
            message = "Unable to set VisibilityTimeout. " \
                      "Message might have previously been deleted upon completion or failure"
            log_job_message(logger=self._logger, message=message, is_exception=True)
            raise QueueWorkDispatcherError(message) from exc

    def _signal_handler(self, signum, frame):
        """Custom handler code to execute when the parent dispatcher process receives a signal.

        Allows the script to update/release jobs and then gracefully exit.
        """
        try:
            self._halt_job_with_signal(signum, False)
        finally:
            raise SystemExit  # quietly end parent dispatcher process

    def _halt_job_with_signal(self, signum, is_worker):
        """
        Attempt to gracefully handle the halting of the job as a result of an event indicated by the signal

        The signal very likely indicates a non-error failure scenario from which the job might be given a retry,
        if allowed. Handle logging, retry logic, and

        :param int signum: Signal
        :param bool is_worker: Whether this signal was received by the worker process (True), or the parent dispatcher
               process (False)
        :raises QueueWorkerProcessError: When the signal was received by the child worker process
        :raises QueueWorkDispatcherError: When the signal was received by the parent dispatcher process
        :return: None
        """
        # If the signal is in BSD_SIGNALS, use the human-readable string, otherwise use the signal value
        signal_or_human = BSD_SIGNALS.get(signum, signum)
        proc_label = "Worker" if is_worker else "Parent Dispatcher"
        message = "{} process received signal [{}]. " \
                  "Gracefully stopping job being worked".format(proc_label, signal_or_human)
        log_job_message(logger=self._logger, message=message, is_error=True)
        if self.allow_retries:
            self.surrender_message_to_other_consumers()
        else:  # otherwise, attempt to send directly to the dead letter queue
            self.move_message_to_dead_letter_queue()
        if is_worker:
            raise QueueWorkerProcessError(message)
        else:
            raise QueueWorkDispatcherError(message)


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
