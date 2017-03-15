from contextlib import contextmanager
import logging

from celery import Celery
from celery.exceptions import MaxRetriesExceededError
from flask import Flask
import requests

from dataactcore.config import CONFIG_DB, CONFIG_SERVICES, CONFIG_JOB_QUEUE
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.logging import configure_logging
from dataactcore.models.jobModels import Job
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.utils import fileE, fileF
from dataactvalidator.filestreaming.csv_selection import write_csv


logger = logging.getLogger(__name__)


def broker_url(host):
    """We use a different broker_url when running the workers than when
    running within the flask app. Generate an appropriate URL with that in
    mind"""
    return '{broker_scheme}://{username}:{password}@{host}:{port}//'.format(host=host, **CONFIG_JOB_QUEUE)


# Set up backend persistent URL
backendUrl = 'db+{scheme}://{username}:{password}@{host}/{job_queue_db_name}'.format(**CONFIG_DB)
celery_app = Celery('tasks', backend=backendUrl, broker=broker_url(CONFIG_JOB_QUEUE['url']))
celery_app.config_from_object('celeryconfig')


@celery_app.task(name='jobQueue.enqueue')
def enqueue(job_id):
    """POST a job to the validator"""
    logger.info('Adding job %s to the queue', job_id)
    validator_url = '{validator_host}:{validator_port}'.format(**CONFIG_SERVICES)
    if 'http://' not in validator_url:
        validator_url = 'http://' + validator_url
    validator_url += '/validate/'
    params = {'job_id': job_id}
    response = requests.post(validator_url, json=params)
    logger.info('Job %s has completed validation', job_id)
    logger.info('Validator response: %s', response.json())
    return response.json()


@contextmanager
def job_context(task, job_id):
    """Common context for file E and F generation. Handles marking the job
    finished and/or failed"""
    # Flask context ensures we have access to global.g
    with Flask(__name__).app_context():
        sess = GlobalDB.db().session
        try:
            yield sess
            logger.debug('Marking job as finished')
            mark_job_status(job_id, "finished")
        except Exception as e:
            logger.debug('EXCEPTION CAUGHT')
            # logger.exception() automatically adds traceback info
            logger.exception('Job %s failed, retrying', job_id)
            try:
                raise task.retry()
            except MaxRetriesExceededError:
                logger.warning('Job %s completely failed', job_id)
                # Log the error
                job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
                if job:
                    job.error_message = str(e)
                    sess.commit()
                    mark_job_status(job_id, "failed")
        finally:
            GlobalDB.close()


@celery_app.task(name='jobQueue.generate_f_file', max_retries=0, bind=True)
def generate_f_file(task, submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write rows from fileF.generate_f_rows to an appropriate CSV."""

    logger.debug('Starting file F generation')

    with job_context(task, job_id):
        logger.debug('Calling genearte_f_rows')
        rows_of_dicts = fileF.generate_f_rows(submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        logger.debug('Writing file F CSV')
        write_csv(timestamped_name, upload_file_name, is_local, header, body)

    logger.debug('Finished file F generation')


@celery_app.task(name='jobQueue.generate_e_file', max_retires=3, bind=True)
def generate_e_file(task, submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write file E to an appropriate CSV."""
    with job_context(task, job_id) as session:
        d1 = session.\
            query(AwardProcurement.awardee_or_recipient_uniqu).\
            filter(AwardProcurement.submission_id == submission_id).\
            distinct()
        d2 = session.\
            query(AwardFinancialAssistance.awardee_or_recipient_uniqu).\
            filter(AwardFinancialAssistance.submission_id == submission_id).\
            distinct()
        duns_set = {r.awardee_or_recipient_uniqu for r in d1.union(d2)}
        duns_list = list(duns_set)    # get an order

        rows = []
        for i in range(0, len(duns_list), 100):
            rows.extend(fileE.retrieve_rows(duns_list[i:i + 100]))
        write_csv(timestamped_name, upload_file_name, is_local, fileE.Row._fields, rows)


if __name__ in ['__main__', 'jobQueue']:
    configure_logging()
    celery_app.conf.update(BROKER_URL=broker_url('localhost'))
