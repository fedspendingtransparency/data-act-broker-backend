import logging

from dataactcore.interfaces import db
from dataactcore.models.jobModels import Job

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

sess = db.dbConnection().session
job = sess.query(Job).filter(Job.job_id == 1).one()

logger.info('Show data from lazy=joined relationship (job_type): {}'.format(job.job_type.name))
logger.info('Show data from lazy=None relationship (file_type): {}'.format(job.file_type.name))