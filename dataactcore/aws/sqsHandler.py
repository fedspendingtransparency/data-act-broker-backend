import boto3
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.jobModels import SQS
from dataactcore.interfaces.db import GlobalDB


class SQSMockQueue:
    @staticmethod
    def send_message(MessageBody):    # noqa
        sess = GlobalDB.db().session
        sess.add(SQS(job_id=int(MessageBody)))
        sess.commit()
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    @staticmethod
    def receive_messages(WaitTimeSeconds):  # noqa
        sess = GlobalDB.db().session
        messages = []
        for sqs in sess.query(SQS):
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
        self.body = sqs.job_id

    def delete(self):
        sess = GlobalDB.db().session
        sess.delete(self.sqs)
        sess.commit()


def sqs_queue():
    if CONFIG_BROKER['local']:
        return SQSMockQueue
    else:
        # stuff that's in get_queue
        sqs = boto3.resource('sqs', region_name=CONFIG_BROKER['aws_region'])
        queue = sqs.get_queue_by_name(QueueName=CONFIG_BROKER['sqs_queue_name'])
        return queue
