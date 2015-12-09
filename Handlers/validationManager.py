class ValidationManager:
    """ Outer level class, called by flask route
    """

    def validateJob(self, jobId):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        jobId:

        Returns:

        """

        # Check that this is a csv file
        # Save number of columns
        # For each row, check number of columns, then pull list of validations from DB and call validator for each one
        # If valid, write to staging DB