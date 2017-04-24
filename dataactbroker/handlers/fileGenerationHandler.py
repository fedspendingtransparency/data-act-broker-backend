from contextlib import contextmanager
import logging

from flask import Flask

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import mark_job_status
from dataactcore.models.jobModels import Job
from dataactcore.models.stagingModels import AwardFinancialAssistance, AwardProcurement
from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.utils import fileE, fileF
from dataactvalidator.filestreaming.csv_selection import write_csv


logger = logging.getLogger(__name__)


@contextmanager
def job_context(job_id):
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
            # logger.exception() automatically adds traceback info
            logger.exception('Job %s failed', job_id)
            job = sess.query(Job).filter_by(job_id=job_id).one_or_none()
            if job:
                job.error_message = str(e)
                sess.commit()
                mark_job_status(job_id, "failed")
        finally:
            GlobalDB.close()


def generate_f_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write rows from fileF.generate_f_rows to an appropriate CSV."""

    logger.debug('Starting file F generation')

    with job_context(job_id):
        logger.debug('Calling genearte_f_rows')
        rows_of_dicts = fileF.generate_f_rows(submission_id)
        header = [key for key in fileF.mappings]    # keep order
        body = []
        for row in rows_of_dicts:
            body.append([row[key] for key in header])

        logger.debug('Writing file F CSV')
        write_csv(timestamped_name, upload_file_name, is_local, header, body)

    logger.debug('Finished file F generation')


def generate_e_file(submission_id, job_id, timestamped_name, upload_file_name, is_local):
    """Write file E to an appropriate CSV."""
    with job_context(job_id) as session:
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

        # Add rows to database here.
        # TODO: This is a temporary solution until loading from SAM's SFTP has been resolved
        for row in rows:
            session.merge(ExecutiveCompensation(**fileE.row_to_dict(row)))
        session.commit()

        write_csv(timestamped_name, upload_file_name, is_local, fileE.Row._fields, rows)
