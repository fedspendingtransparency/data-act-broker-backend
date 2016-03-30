from celery import Celery
import requests

# params need to be in a broker config file
# TODO: add persistant queue to be available across server restarts
jobQueue = Celery('tasks', backend='rpc://', broker='amqp://user:pass@ec2-52-200-1-10.compute-1.amazonaws.com:5672//')

@jobQueue.task(name='jobQueue.enqueue')
def enqueue(jobID):
    # Don't need to worry about the response currently
    url = 'http://ec2-52-90-92-100.compute-1.amazonaws.com/validate/'
    params = {
        'job_id': jobID
    }
    response = requests.post(url,params)
    return response.json()