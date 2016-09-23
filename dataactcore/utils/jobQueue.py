from celery import Celery
from flask import Flask
import requests

from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE
from dataactcore.utils import fileF
from dataactcore.utils.cloudLogger import CloudLogger
from dataactvalidator.filestreaming.csv_selection import write_csv


def brokerUrl(host):
    """We use a different brokerUrl when running the workers than when
    running within the flask app. Generate an appropriate URL with that in
    mind"""
    return '{broker_scheme}://{username}:{password}@{host}:{port}//'.format(
        host=host, **CONFIG_JOB_QUEUE)


# Set up backend persistent URL
backendUrl = ('db+{scheme}://{username}:{password}@{host}'
              '/{job_queue_db_name}').format(**CONFIG_DB)
celery_app = Celery('tasks', backend=backendUrl,
                    broker=brokerUrl(CONFIG_JOB_QUEUE['url']))
celery_app.config_from_object('celeryconfig')


@celery_app.task(name='jobQueue.enqueue')
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


@celery_app.task(name='jobQueue.generate_f_file')
def generate_f_file(submission_id, job_id, interface_holder_class,
                    timestamped_name, is_local):
    """Write rows from fileF.generateFRows to an appropriate CSV. Here the
    third parameter, interface_holder_class, is a bit of a hack. Importing
    InterfaceHolder directly causes cyclic dependency woes, so we're passing
    in a class"""
    # Setup a Flask context
    with Flask(__name__).app_context():
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


if __name__ in ['__main__', 'jobQueue']:
    celery_app.conf.update(BROKER_URL=brokerUrl('localhost'))
