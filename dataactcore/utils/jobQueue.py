from celery import Celery
from flask import Flask
import requests

from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE
from dataactcore.utils import fileF
from dataactcore.utils.cloudLogger import CloudLogger
from dataactvalidator.filestreaming.csv_selection import write_csv


# We'll use this to make sure our database connection is safe. Hopefully we'll
# avoid the need in the future
job_app = Flask(__name__)


def enqueue(jobID):
    """POST a job to the validator"""
    CloudLogger.log("Adding job {} to the queue".format(str(jobID)))
    validatorUrl = '{validator_host}:{validator_port}'.format(
        **CONFIG_SERVICES)
    if 'http://' not in validatorUrl:
        validatorUrl = 'http://' + validatorUrl
    validatorUrl += '/validate/'
    params = {
        'job_id': jobID
    }
    response = requests.post(validatorUrl, params)
    CloudLogger.log("Job {} has completed validation".format(str(jobID)))
    CloudLogger.log("Validator response: {}".format(str(response.json())))
    return response.json()


def generate_f_file(submission_id, job_id, interface_holder_class,
                    timestamped_name, is_local):
    """Write rows from fileF.generateFRows to an appropriate CSV. Here the
    third parameter, interface_holder_class, is a bit of a hack. Importing
    InterfaceHolder directly causes cyclic dependency woes, so we're passing
    in a class"""
    with job_app.app_context():
        job_manager = interface_holder_class().jobDb

        try:
            rows_of_dicts = fileF.generateFRows(job_manager.session,
                                                submission_id)
            header = [key for key in fileF.mappings]    # keep order
            body = []
            for row in rows_of_dicts:
                body.append([row[key] for key in header])

            write_csv(timestamped_name, is_local, header, body)
            job_manager.markJobStatus(job_id, "finished")
        except Exception as e:
            # Log the error
            job_manager.getJobById(job_id).error_message = str(e)
            job_manager.markJobStatus(job_id, "failed")
            job_manager.session.commit()

        job_manager.close()


class JobQueue:
    def __init__(self, job_queue_url="localhost"):
        # Set up backend persistent URL
        backendUrl = ('db+{scheme}://{username}:{password}@{host}'
                      '/{job_queue_db_name}').format(**CONFIG_DB)

        # Set up url to the job queue to establish connection
        queueUrl = ('{broker_scheme}://{username}:{password}@{host}:{port}'
                    '//').format(host=job_queue_url, **CONFIG_JOB_QUEUE)

        # Create remote connection to the job queue
        self.jobQueue = Celery('tasks', backend=backendUrl, broker=queueUrl)
        self.jobQueue.config_from_object('celeryconfig')

        self.enqueue = self.jobQueue.task(name='jobQueue.enqueue')(enqueue)
        self.generate_f_file = self.jobQueue.task(
            name='jobQueue.generate_f_file')(generate_f_file)

if __name__ in ['__main__', 'jobQueue']:
    jobQueue = JobQueue()
    queue = jobQueue.jobQueue
