from celery import Celery
from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE
import requests

# Set up backend persistent URL
backendUrl = ''.join(['db+', CONFIG_DB['scheme'], '://', CONFIG_DB['username'], ':', CONFIG_DB['password'], '@', CONFIG_DB['host'], '/', CONFIG_DB['job_queue_db_name']])

# Set up url to the validator for the RESTFul calls
validatorUrl = ''.join([CONFIG_SERVICES['validator_host'], ':', str(CONFIG_SERVICES['validator_port'])])
if 'http://' not in validatorUrl:
	validatorUrl = ''.join(['http://', validatorUrl])

# Set up url to the job queue to establish connection
queueUrl = ''.join([CONFIG_JOB_QUEUE['broker_scheme'], '://', CONFIG_JOB_QUEUE['username'], ':', CONFIG_JOB_QUEUE['password'], '@', CONFIG_JOB_QUEUE['url'], ':', str(CONFIG_JOB_QUEUE['port']), '//'])

# Create remote connection to the job queue
jobQueue = Celery('tasks', backend=backendUrl, broker=queueUrl)

@jobQueue.task(name='jobQueue.enqueue')
def enqueue(jobID):
    # Don't need to worry about the response currently
    url = ''.join([validatorUrl, '/validate/'])
    params = {
        'job_id': jobID
    }
    response = requests.post(url,params)
    return response.json()