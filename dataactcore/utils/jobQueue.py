from celery import Celery
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.config import CONFIG_SERVICES, CONFIG_JOB_QUEUE
import requests

# Set up backend persistent URL
creds = JobTrackerInterface.getCredDict()
dbScheme = creds['scheme'] if 'scheme' in creds else 'postgres'
dbName = 'job_queue'
backendUrl = ''.join(['db+', dbScheme, '://', creds['username'], ':', creds['password'], '@', creds['host'], '/', dbName])

# Set up url to the validator for the RESTFul calls
validatorUrl = ''.join(['http://', CONFIG_SERVICES['validator_host'], ':', str(CONFIG_SERVICES['validator_port'])])

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