import boto3
from itertools import islice
import json

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.jobModels import SQS
from dataactcore.interfaces.db import GlobalDB


class SQSMockQueue:
    @staticmethod
    def send_message(MessageBody, MessageAttributes=None):  # noqa
        sess = GlobalDB.db().session
        sess.add(SQS(message=int(MessageBody), attributes=str(MessageAttributes) if MessageAttributes else None))
        sess.commit()
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    @staticmethod
    def receive_messages(WaitTimeSeconds, AttributeNames=None, MessageAttributeNames=None,   # noqa
                         VisibilityTimeout=30, MaxNumberOfMessages=1):  # noqa
        sess = GlobalDB.db().session
        messages = []
        # Limit returned messages by MaxNumberOfMessages: start=0, stop=MaxNumberOfMessages
        for sqs in islice(sess.query(SQS).order_by(SQS.sqs_id), 0, MaxNumberOfMessages):
            messages.append(SQSMockMessage(sqs))
        return messages

    @staticmethod
    def purge():
        sess = GlobalDB.db().session
        sess.query(SQS).delete()
        sess.commit()


class SQSMockMessage:
    def __init__(self, sqs):
        self.sqs = sqs
        self.body = sqs.message
        self.message_attributes = json.loads(sqs.attributes.replace("'", '"')) if sqs.attributes else None

    def delete(self):
        sess = GlobalDB.db().session
        sess.delete(self.sqs)
        sess.commit()

    def change_visibility(self, VisibilityTimeout): # noqa
        # Do nothing
        pass

    @property
    def attributes(self):  # TODO: May need to do more handling of this to account for ApproximateReceiveCount attr
        return {}


def sqs_queue(region_name=CONFIG_BROKER['aws_region'], queue_name=CONFIG_BROKER['sqs_queue_name']):
    if CONFIG_BROKER['local']:
        return SQSMockQueue
    else:
        # stuff that's in get_queue
        sqs = boto3.resource('sqs', region_name)
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        return queue
