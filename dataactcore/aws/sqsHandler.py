import boto3
from dataactcore.config import CONFIG_BROKER


def get_queue():
    sqs = boto3.resource('sqs', region_name=CONFIG_BROKER['aws_region'])
    queue = sqs.get_queue_by_name(QueueName=CONFIG_BROKER['sqs_queue_name'])
    return queue
