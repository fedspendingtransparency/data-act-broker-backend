from contextlib import contextmanager
import logging

from celery import Celery
from celery.exceptions import MaxRetriesExceededError
from flask import Flask
import requests

from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE
from dataactcore.models.stagingModels import (
    AwardFinancialAssistance, AwardProcurement)
from dataactcore.utils import fileE, fileF
from dataactcore.utils.cloudLogger import CloudLogger
from dataactvalidator.filestreaming.csv_selection import write_csv


logger = logging.getLogger(__name__)


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


@contextmanager
def job_context(task, interface_holder_class, job_id):
    """Common context for file E and F generation. Handles marking the job
    finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        job_manager = interface_holder_class().jobDb

        try:
            yield job_manager
            job_manager.markJobStatus(job_id, "finished")
        except Exception as e:
            # logger.exception() automatically adds traceback info
            logger.exception('Job %s failed, retrying', job_id)
            try:
                raise task.retry()
            except MaxRetriesExceededError:
                logger.warning('Job %s completely failed', job_id)
                # Log the error
                job_manager.getJobById(job_id).error_message = str(e)
                job_manager.markJobStatus(job_id, "failed")

        job_manager.close()


@celery_app.task(name='jobQueue.generate_f_file', max_retries=0, bind=True)
def generate_f_file(task, submission_id, job_id, interface_holder_class,
                    timestamped_name, upload_file_name, is_local):
    """Write rows from fileF.generateFRows to an appropriate CSV. Here the
    third parameter, interface_holder_class, is a bit of a hack. Importing
    InterfaceHolder directly causes cyclic dependency woes, so we're passing
    in a class"""
    with job_context(task, interface_holder_class, job_id) as job_manager:
        rows_of_dicts = fileF.generateFRows(job_manager.session,
                                            submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        write_csv(timestamped_name, upload_file_name, is_local, header,
                  body)


@celery_app.task(name='jobQueue.generate_e_file', max_retires=3, bind=True)
def generate_e_file(task, submission_id, job_id, interface_holder_class,
                    timestamped_name, upload_file_name, is_local):
    """Write file E to an appropriate CSV. See generate_file_file for an
    explanation of interface_holder_class"""
    with job_context(task, interface_holder_class, job_id) as job_manager:
        d1 = job_manager.session.\
            query(AwardProcurement.awardee_or_recipient_uniqu).\
            filter(AwardProcurement.submission_id == submission_id).\
            distinct()
        d2 = job_manager.session.\
            query(AwardFinancialAssistance.awardee_or_recipient_uniqu).\
            filter(AwardFinancialAssistance.submission_id == submission_id).\
            distinct()
        dunsSet = {r.awardee_or_recipient_uniqu for r in d1.union(d2)}

        write_csv(timestamped_name, upload_file_name, is_local,
                  fileE.Row._fields, fileE.retrieveRows(list(dunsSet)))


if __name__ in ['__main__', 'jobQueue']:
    celery_app.conf.update(BROKER_URL=brokerUrl('localhost'))
