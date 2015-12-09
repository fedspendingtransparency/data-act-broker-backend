import sqlalchemy
import json

class JobTrackerInterface:
    """ Manages all interaction with the job tracker database
    """
    dbConfigFile = "jobTracker.conf"

    def __init__(self):
        """ Set up connection to job tracker database


        """


    def checkPrerequisites(self, jobId):
        """ Checks that specified job has no unsatisfied prerequisites
        Args:
        jobId -- job_id of job to be run

        Returns:
        True if prerequisites are satisfied, False otherwise
        """