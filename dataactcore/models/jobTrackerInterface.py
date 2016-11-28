import logging

from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import sumNumberOfErrorsForJobList
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.jobModels import Job, JobStatus
from dataactcore.models.lookups import JOB_STATUS_DICT


_exception_logger = logging.getLogger('deprecated.exception')


class JobTrackerInterface(BaseInterface):
    """Manages all interaction with the job tracker database."""
    def checkJobUnique(self, query):
        """ Checks if sqlalchemy queryResult has only one entry, error messages are specific to unique jobs

        Args:
        queryResult -- sqlalchemy query result

        Returns:
        True if single result, otherwise exception
        """
        return self.runUniqueQuery(query, "Job ID not found in job table","Conflicting jobs found for this ID")

