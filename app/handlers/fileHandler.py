import json
import sys
import traceback
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.utils.requestDictionary import RequestDictionary
from jobHandler import JobHandler
from errorHandler import ErrorHandler
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException

from managerProxy import ManagerProxy



class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    s3manager -- instance of s3UrlHandler, manages calls to S3
    """


    FILE_TYPES = ["appropriations","award_financial","award","procurement"]

    def __init__(self,request):
        """

        Arguments:
        jobManager -- A JobManager object to interact with job tracker database
        """
        self.jobManager = JobHandler()
        self.request = request # Should be added for each request by calling setRequest


    def getErrorReportURL(self):
        """
        Gets the Signed URL for download based on the jobId
        """
        try :
            self.s3manager = s3UrlHandler(s3UrlHandler.getBucketNameFromConfig())
            safeDictionary = RequestDictionary(self.request)
            responseDict ={}
            responseDict["error_url"] = self.s3manager.getSignedUrl("errors",self.jobManager.getReportPath(safeDictionary.getValue("upload_id")),"GET")
            return JsonResponse.create(StatusCode.OK,responseDict)
        except ResponseException as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
    # Submit set of files
    def submit(self,name):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
        name -- User ID from the session handler

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        self.s3manager = s3UrlHandler(s3UrlHandler.getBucketNameFromConfig())
        responseDict= {}
        try:
            jobManager = JobHandler()
            fileNameMap = []

            safeDictionary = RequestDictionary(self.request)
            for fileName in FileHandler.FILE_TYPES :
                if( safeDictionary.exists(fileName)) :
                    responseDict[fileName+"_url"] = self.s3manager.getSignedUrl(str(name),safeDictionary.getValue(fileName))
                    fileNameMap.append((fileName,self.s3manager.s3FileName))

            fileJobDict = jobManager.createJobs(fileNameMap)
            for fileName in fileJobDict.keys():
                if (not "submission_id" in fileName) :
                    responseDict[fileName+"_id"] = fileJobDict[fileName]

            responseDict["submission_id"] = fileJobDict["submission_id"]
            return JsonResponse.create(StatusCode.OK,responseDict)
        except (ValueError , TypeError, NotImplementedError) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def finalize(self):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        responseDict = {}
        try:
            inputDictionary = RequestDictionary(self.request)
            jobId = inputDictionary.getValue("upload_id")
            # Change job status to finished
            jobManager = JobHandler()
            if(jobManager.checkUploadType(jobId)):
                jobManager.changeToFinished(jobId)
                responseDict["success"] = True
                proxy =  ManagerProxy()
                proxy.sendJobRequest(jobId)
                return JsonResponse.create(StatusCode.OK,responseDict)
            else:
                exc = ResponseException("Wrong job type for finalize route")
                exc.status = 400
                raise exc

        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def getJobsBySubmission(self,submissionId):
        """ Get list of jobs that are part of the specified submission

        Args:
            submissionId: submission to list jobs for

        Returns:
            List of job IDs
        """
        jobList = []
        queryResult = self.session.query(JobStatus.job_id).filter(JobStatus.submission_id == submissionId).all()
        for result in queryResult:
            jobList.append(result.job_id)
        return jobList

    def getErrorMeterics(self) :
        responseDict = {}
        returnDict = {}
        try:
            safeDictionary = RequestDictionary(self.request)
            submission_id =  safeDictionary.getValue("submission_id")
            jobIds = self.jobManager.getJobsBySubmission(submission_id)
            for currentId in jobIds :
                fileName = self.jobManager.getFileType(currentId)
                errorHandler = ErrorHandler()
                dataList = errorHandler.getErrorMetericsByJobId(currentId)
                returnDict[fileName]  = dataList
            return JsonResponse.create(StatusCode.OK,returnDict)
        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
