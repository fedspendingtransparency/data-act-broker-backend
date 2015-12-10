import sys, os, inspect
from dataactcore.utils.jsonResponse import JsonResponse
from interfaces.jobTrackerInterface import JobTrackerInterface
import struct
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.responseException import ResponseException

class ValidationManager:
    """ Outer level class, called by flask route
    """

    def validateJob(self, request):
        """ Gets file for job, validates each row, and sends valid rows to staging database
        Args:
        request -- HTTP request containing the jobId

        Returns:
        Http response object
        """
        try:
            requestDict = RequestDictionary(request)
            if(requestDict.exists("job_id")):
                jobId = requestDict.getValue("job_id")
            else:
                # Request does not have a job ID, can't validate
                exc = ResponseException("No job ID specified in request")
                exc.status = 400
                raise exc
            # Create connection to job tracker database
            jobTracker = JobTrackerInterface()
            # Check that job exists and is ready
            if(not (jobTracker.runChecks(jobId))):
                exc = ResponseException("Checks failed on Job ID")
                exc.status = 400
                raise exc

            # Pull file from S3
            # Check that this has a csv extension?

            # For now just use a local file
            path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
            awardFile = open(path + "/awardValid.csv","r")

            # Read first row into an array of column names
            # Save number of columns
            # For each row, check number of columns, then pull list of validations from DB and call validator for each one
            # If valid, write to staging DB
            # Mark validation as finished in job tracker
            jobTracker.markFinished(jobId)
            return JsonResponse.create(200,{})
        except ResponseException as e:
            return JsonResponse.error(e,e.status)

if __name__ == '__main__':
    validManager = ValidationManager()
    validManager.validateJob(1)