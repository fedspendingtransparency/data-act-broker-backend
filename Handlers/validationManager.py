class ValidationManager:
    """ Outer level class, called by flask route
    """

    def validateJob(self, jobId):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        jobId:

        Returns:

        """