from celery import Celery
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.config import CONFIG_SERVICES
import requests

creds = BaseInterface.getCredDict()
dbScheme = creds['scheme'] if 'scheme' in creds else 'postgres'
dbName = 'job_queue'
backendUrl = ''.join(['db+', dbScheme, '://', creds['username'], ':', creds['password'], '@', creds['host'], '/', dbName])
jobQueue = Celery('tasks', backend=backendUrl, broker='amqp://user:pass@ec2-52-200-1-10.compute-1.amazonaws.com:5672//')
validatorUrl = ''.join(['http://', CONFIG_SERVICES['validator_host'], ':', CONFIG_SERVICES['validator_port']])

@jobQueue.task(name='jobQueue.enqueue')
def enqueue(jobID):
    # Don't need to worry about the response currently
    url = ''.join([validatorUrl, '/validate/'])
    params = {
        'job_id': jobID
    }
    response = requests.post(url,params)
    return response.json()