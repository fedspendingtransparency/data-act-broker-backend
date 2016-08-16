import os
from flask import session, request
from datetime import datetime
from werkzeug import secure_filename
from dataactcore.aws.s3UrlHandler import s3UrlHandler
from dataactcore.utils.requestDictionary import RequestDictionary
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.stringCleaner import StringCleaner
from dataactcore.config import CONFIG_BROKER, CONFIG_JOB_QUEUE
from dataactbroker.handlers.aws.session import LoginSession
from dataactcore.utils.jobQueue import JobQueue
from sqlalchemy.orm.exc import NoResultFound
from dataactbroker.handlers.interfaceHolder import InterfaceHolder


class FileHandler:
    """ Responsible for all tasks relating to file upload

    Static fields:
    FILE_TYPES -- list of file labels that can be included

    Instance fields:
    request -- A flask request object, comes with the request
    s3manager -- instance of s3UrlHandler, manages calls to S3
    """

    FILE_TYPES = ["appropriations","award_financial","program_activity"]
    EXTERNAL_FILE_TYPES = ["award"] #,"award_procurement"]
    VALIDATOR_RESPONSE_FILE = "validatorResponse"

    def __init__(self,request,interfaces = None,isLocal= False,serverPath =""):
        """ Create the File Handler

        Arguments:
            request - HTTP request object for this route
            interfaces - InterfaceHolder object to databases
            isLocal - True if this is a local installation that will not use AWS
            serverPath - If isLocal is True, this is used as the path to local files
        """
        self.request = request
        if(interfaces != None):
            self.interfaces = interfaces
            self.jobManager = interfaces.jobDb
        self.isLocal = isLocal
        self.serverPath = serverPath
        self.s3manager = s3UrlHandler()

    def addInterfaces(self,interfaces):
        """ Add connections to databases

        Args:
            interfaces: InterfaceHolder object to DBs
        """
        self.interfaces = interfaces
        self.jobManager = interfaces.jobDb

    def getErrorReportURLsForSubmission(self, isWarning = False):
        """
        Gets the Signed URLs for download based on the submissionId
        """
        try :
            self.s3manager = s3UrlHandler()
            safeDictionary = RequestDictionary(self.request)
            submissionId = safeDictionary.getValue("submission_id")
            responseDict ={}
            for jobId in self.jobManager.getJobsBySubmission(submissionId):
                if(self.jobManager.getJobType(jobId) == "csv_record_validation"):
                    if isWarning:
                        reportName = self.jobManager.getWarningReportPath(jobId)
                        key = "job_"+str(jobId)+"_warning_url"
                    else:
                        reportName = self.jobManager.getReportPath(jobId)
                        key = "job_"+str(jobId)+"_error_url"
                    if(not self.isLocal):
                        responseDict[key] = self.s3manager.getSignedUrl("errors",reportName,method="GET")
                    else:
                        path = os.path.join(self.serverPath, reportName)
                        responseDict[key] = path

            # For each pair of files, get url for the report
            fileTypes = self.interfaces.validationDb.getFileTypeList()
            for source in fileTypes:
                sourceId = self.interfaces.validationDb.getFileTypeIdByName(source)
                for target in fileTypes:
                    targetId = self.interfaces.validationDb.getFileTypeIdByName(target)
                    if targetId <= sourceId:
                        # Skip redundant reports
                        continue
                    # Retrieve filename
                    if isWarning:
                        reportName = self.interfaces.errorDb.getCrossWarningReportName(submissionId, source, target)
                    else:
                        reportName = self.interfaces.errorDb.getCrossReportName(submissionId, source, target)
                    # If not local, get a signed URL
                    if self.isLocal:
                        reportPath = os.path.join(self.serverPath,reportName)
                    else:
                        reportPath = self.s3manager.getSignedUrl("errors",reportName,method="GET")
                    # Assign to key based on source and target
                    responseDict[self.getCrossReportKey(source,target,isWarning)] = reportPath

            return JsonResponse.create(StatusCode.OK,responseDict)
        except ResponseException as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def getCrossReportKey(self,sourceType,targetType,isWarning = False):
        """ Generate a key for cross-file error reports """
        if isWarning:
            return "cross_warning_{}-{}".format(sourceType,targetType)
        else:
            return "cross_{}-{}".format(sourceType,targetType)

    # Submit set of files
    def submit(self,name,CreateCredentials):
        """ Builds S3 URLs for a set of files and adds all related jobs to job tracker database

        Flask request should include keys from FILE_TYPES class variable above

        Arguments:
            name -- User ID from the session handler
            CreateCredentials - If True, will create temporary credentials for S3 uploads

        Returns:
        Flask response returned will have key_url and key_id for each key in the request
        key_url is the S3 URL for uploading
        key_id is the job id to be passed to the finalize_submission route
        """
        try:
            responseDict= {}

            fileNameMap = []
            safeDictionary = RequestDictionary(self.request)
            submissionId = self.jobManager.createSubmission(name, safeDictionary)
            existingSubmission = False
            if safeDictionary.exists("existing_submission_id"):
                existingSubmission = True
                # Check if user has permission to specified submission
                self.checkSubmissionPermission(self.jobManager.getSubmissionById(submissionId))

            # Build fileNameMap to be used in creating jobs
            for fileType in FileHandler.FILE_TYPES :
                # If filetype not included in request, and this is an update to an existing submission, skip it
                if not safeDictionary.exists(fileType):
                    if existingSubmission:
                        continue
                    # This is a new submission, all files are required
                    raise ResponseException("Must include all files for new submission", StatusCode.CLIENT_ERROR)

                filename = safeDictionary.getValue(fileType)
                if( safeDictionary.exists(fileType)) :
                    if(not self.isLocal):
                        uploadName =  str(name)+"/"+s3UrlHandler.getTimestampedFilename(filename)
                    else:
                        uploadName = filename
                    responseDict[fileType+"_key"] = uploadName
                    fileNameMap.append((fileType,uploadName,filename))

            if not fileNameMap and existingSubmission:
                raise ResponseException("Must include at least one file for an existing submission",
                                        StatusCode.CLIENT_ERROR)

            for extFileType in FileHandler.EXTERNAL_FILE_TYPES:
                if extFileType == "award":
                    filename = CONFIG_BROKER["d2_file_name"]
                elif extFileType == "award_procurement":
                    filename = CONFIG_BROKER["d1_file_name"]

                if(not self.isLocal):
                    uploadName = str(name) + "/" + s3UrlHandler.getTimestampedFilename(filename)
                else:
                    uploadName = filename
                responseDict[extFileType + "_key"] = uploadName
                fileNameMap.append((extFileType, uploadName, filename))

            fileJobDict = self.jobManager.createJobs(fileNameMap,submissionId,existingSubmission)
            for fileType in fileJobDict.keys():
                if (not "submission_id" in fileType) :
                    responseDict[fileType+"_id"] = fileJobDict[fileType]
            if(CreateCredentials and not self.isLocal) :
                self.s3manager = s3UrlHandler(CONFIG_BROKER["aws_bucket"])
                responseDict["credentials"] = self.s3manager.getTemporaryCredentials(name)
            else :
                responseDict["credentials"] ={"AccessKeyId" : "local","SecretAccessKey" :"local","SessionToken":"local" ,"Expiration" :"local"}

            responseDict["submission_id"] = fileJobDict["submission_id"]
            if self.isLocal:
                responseDict["bucket_name"] = CONFIG_BROKER["broker_files"]
            else:
                responseDict["bucket_name"] = CONFIG_BROKER["aws_bucket"]
            return JsonResponse.create(StatusCode.OK,responseDict)
        except (ValueError , TypeError, NotImplementedError) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            # Call error route directly, status code depends on exception
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)
        except:
            return JsonResponse.error(Exception("Failed to catch exception"),StatusCode.INTERNAL_ERROR)

    def finalize(self, jobId=None):
        """ Set upload job in job tracker database to finished, allowing dependent jobs to be started

        Flask request should include key "upload_id", which holds the job_id for the file_upload job

        Returns:
        A flask response object, if successful just contains key "success" with value True, otherwise value is False
        """
        responseDict = {}
        try:
            if jobId is None:
                inputDictionary = RequestDictionary(self.request)
                jobId = inputDictionary.getValue("upload_id")

            # Compare user ID with user who submitted job, if no match return 400
            job = self.jobManager.getJobById(jobId)
            submission = self.jobManager.getSubmissionForJob(job)
            # Check that user's agency matches submission cgac_code or "SYS", or user id matches submission's user
            userId = LoginSession.getName(session)
            userCgac = self.interfaces.userDb.getUserByUID(userId).cgac_code
            if(submission.user_id != userId and submission.cgac_code != userCgac and userCgac != "SYS"):
                # This user cannot finalize this job
                raise ResponseException("Cannot finalize a job for a different agency", StatusCode.CLIENT_ERROR)
            # Change job status to finished
            if(self.jobManager.checkUploadType(jobId)):
                self.jobManager.changeToFinished(jobId)
                responseDict["success"] = True
                return JsonResponse.create(StatusCode.OK,responseDict)
            else:
                raise ResponseException("Wrong job type for finalize route",StatusCode.CLIENT_ERROR)

        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def checkSubmissionPermission(self,submission):
        """ Check if current user has permisson to access submission and return user object.

        Args:
            submission - Submission model object
        """
        userId = LoginSession.getName(session)
        user = self.interfaces.userDb.getUserByUID(userId)
        # Check that user has permission to see this submission, user must be within the agency of the submission, or be
        # the original user, or be in the 'SYS' agency
        submissionCgac = StringCleaner.cleanString(submission.cgac_code)
        userCgac = StringCleaner.cleanString(user.cgac_code)
        if(submissionCgac != userCgac and submission.user_id != user.user_id
           and userCgac != "sys"):
            raise ResponseException("User does not have permission to view that submission",
                StatusCode.CLIENT_ERROR)
        return user

    def getStatus(self):
        """ Get description and status of all jobs in the submission specified in request object

        Returns:
            A flask response object to be sent back to client, holds a JSON where each job ID has a dictionary holding file_type, job_type, status, and filename
        """
        try:
            inputDictionary = RequestDictionary(self.request)

            # Get submission
            submissionId = inputDictionary.getValue("submission_id")
            submission = self.jobManager.getSubmissionById(submissionId)

            # Check that user has access to submission
            user = self.checkSubmissionPermission(submission)

            # Get jobs in this submission
            jobs = self.jobManager.getJobsBySubmission(submissionId)

            # Build dictionary of submission info with info about each job
            submissionInfo = {}
            submissionInfo["jobs"] = []
            submissionInfo["cgac_code"] = submission.cgac_code
            submissionInfo["reporting_period_start_date"] = self.interfaces.jobDb.getStartDate(submission)
            submissionInfo["reporting_period_end_date"] = self.interfaces.jobDb.getEndDate(submission)
            submissionInfo["created_on"] = self.interfaces.jobDb.getFormattedDatetimeBySubmissionId(submissionId)
            # Include number of errors in submission
            submissionInfo["number_of_errors"] = self.interfaces.errorDb.sumNumberOfErrorsForJobList(jobs, self.interfaces.validationDb)
            submissionInfo["number_of_rows"] = self.interfaces.jobDb.sumNumberOfRowsForJobList(jobs)
            submissionInfo["last_updated"] = submission.updated_at.strftime("%Y-%m-%dT%H:%M:%S")

            for jobId in jobs:
                jobInfo = {}
                jobType = self.jobManager.getJobType(jobId)
                if jobType != "csv_record_validation" and jobType != "validation":
                    continue
                jobInfo["job_id"] = jobId
                jobInfo["job_status"] = self.jobManager.getJobStatusName(jobId)
                jobInfo["job_type"] = jobType
                jobInfo["filename"] = self.jobManager.getOriginalFilenameById(jobId)
                try:
                    jobInfo["file_status"] = self.interfaces.errorDb.getFileStatusLabelByJobId(jobId)
                except ResponseException as e:
                    # Job ID not in error database, probably did not make it to validation, or has not yet been validated
                    jobInfo["file_status"] = ""
                    jobInfo["missing_headers"] = []
                    jobInfo["duplicated_headers"] = []
                    jobInfo["error_type"] = ""
                    jobInfo["error_data"] = []
                    jobInfo["warning_data"] = []
                else:
                    # If job ID was found in file, we should be able to get header error lists and file data
                    # Get string of missing headers and parse as a list
                    missingHeaderString = self.interfaces.errorDb.getMissingHeadersByJobId(jobId)
                    if missingHeaderString is not None:
                        # Split header string into list, excluding empty strings
                        jobInfo["missing_headers"] = [n.strip() for n in missingHeaderString.split(",") if len(n) > 0]
                    else:
                        jobInfo["missing_headers"] = []
                    # Get string of duplicated headers and parse as a list
                    duplicatedHeaderString = self.interfaces.errorDb.getDuplicatedHeadersByJobId(jobId)
                    if duplicatedHeaderString is not None:
                        # Split header string into list, excluding empty strings
                        jobInfo["duplicated_headers"] = [n.strip() for n in duplicatedHeaderString.split(",") if len(n) > 0]
                    else:
                        jobInfo["duplicated_headers"] = []
                    jobInfo["error_type"] = self.interfaces.errorDb.getErrorType(jobId)
                    jobInfo["error_data"] = self.interfaces.errorDb.getErrorMetricsByJobId(jobId,jobType=='validation',self.interfaces, severityId=self.interfaces.validationDb.getRuleSeverityId("fatal"))
                    jobInfo["warning_data"] = self.interfaces.errorDb.getErrorMetricsByJobId(jobId,jobType=='validation',self.interfaces, severityId=self.interfaces.validationDb.getRuleSeverityId("warning"))
                # File size and number of rows not dependent on error DB
                # Get file size
                jobInfo["file_size"] = self.jobManager.getFileSizeById(jobId)
                # Get number of rows in file
                jobInfo["number_of_rows"] = self.jobManager.getNumberOfRowsById(jobId)

                try :
                    jobInfo["file_type"] = self.jobManager.getFileType(jobId)
                except Exception as e:
                    jobInfo["file_type"]  = ''
                submissionInfo["jobs"].append(jobInfo)

            # Build response object holding dictionary
            return JsonResponse.create(StatusCode.OK,submissionInfo)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def getErrorMetrics(self) :
        """ Returns an Http response object containing error information for every validation job in specified submission """
        responseDict = {}
        returnDict = {}
        try:
            safeDictionary = RequestDictionary(self.request)
            submission_id =  safeDictionary.getValue("submission_id")

            # Check if user has permission to specified submission
            self.checkSubmissionPermission(self.jobManager.getSubmissionById(submission_id))

            jobIds = self.jobManager.getJobsBySubmission(submission_id)
            for currentId in jobIds :
                if(self.jobManager.getJobType(currentId) == "csv_record_validation"):
                    fileName = self.jobManager.getFileType(currentId)
                    dataList = self.interfaces.errorDb.getErrorMetricsByJobId(currentId)
                    returnDict[fileName]  = dataList
            return JsonResponse.create(StatusCode.OK,returnDict)
        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def uploadFile(self):
        """ Saves a file and returns the saved path.  Should only be used for local installs. """
        try:
            if(self.isLocal):
                uploadedFile = request.files['file']
                if(uploadedFile):
                    seconds = int((datetime.utcnow()-datetime(1970,1,1)).total_seconds())
                    filename = "".join([str(seconds),"_", secure_filename(uploadedFile.filename)])
                    path = os.path.join(self.serverPath, filename)
                    uploadedFile.save(path)
                    returnDict = {"path":path}
                    return JsonResponse.create(StatusCode.OK,returnDict)
                else:
                    exc = ResponseException("Failure to read file", StatusCode.CLIENT_ERROR)
                    return JsonResponse.error(exc,exc.status)
            else :
                exc = ResponseException("Route Only Valid For Local Installs", StatusCode.CLIENT_ERROR)
                return JsonResponse.error(exc,exc.status)
        except ( ValueError , TypeError ) as e:
            return JsonResponse.error(e,StatusCode.CLIENT_ERROR)
        except ResponseException as e:
            return JsonResponse.error(e,e.status)
        except Exception as e:
            # Unexpected exception, this is a 500 server error
            return JsonResponse.error(e,StatusCode.INTERNAL_ERROR)

    def generateD1File(self):
        """ Initiates the generation of D1 """
        requestDict = RequestDictionary(self.request)

        if not (requestDict.exists("submission_id") and requestDict.exists("start") and requestDict.exists("end")):
            exc = ResponseException("Generate D1 Files route requires submission_id, start, and end", StatusCode.CLIENT_ERROR)
            return JsonResponse.create(exc, exc.status)

        submission_id = requestDict.getValue("submission_id")
        start_date = requestDict.getValue("start")
        end_date = requestDict.getValue("end")

        if not (StringCleaner.isNumeric(submission_id) and StringCleaner.isDate(start_date) and StringCleaner.isDate(end_date)):
            exc = ResponseException("submission id, start, and/or end cannot be parsed into their appropriate types", StatusCode.CLIENT_ERROR)
            return JsonResponse.create(exc, exc.status)

        cgac_code = self.jobManager.getSubmissionById(submission_id).cgac_code
        get_url = CONFIG_BROKER["d1_url"].format(cgac_code, start_date, end_date)

        jq = JobQueue(job_queue_url=CONFIG_JOB_QUEUE['url'])

        # Generate and upload D1 file to S3
        user_id = LoginSession.getName(session)
        timestamped_name = s3UrlHandler.getTimestampedFilename(CONFIG_BROKER["d1_file_name"])
        upload_file_name = "".join([str(user_id), "/", timestamped_name])
        d_file_id = self.jobManager.createDFileMeta(submission_id, start_date, end_date, "d1", CONFIG_BROKER["d1_file_name"], upload_file_name)
        self.jobManager.setDFileStatus(d_file_id, "waiting")
        jq.generate_d_file.delay(get_url, CONFIG_BROKER["d1_file_name"], user_id, d_file_id, InterfaceHolder, timestamped_name, skip_gen=True)

        # Check status for D1 file
        return self.checkD1File()

    def checkD1File(self, submission_id=None):
        """ Check status of D1 file generation """
        if submission_id is None:
            requestDict = RequestDictionary(self.request)
            if not (requestDict.exists("submission_id")):
                exc = ResponseException("Check D1 Files route requires submission_id", StatusCode.CLIENT_ERROR)
                return JsonResponse.create(exc, exc.status)

            submission_id = requestDict.getValue("submission_id")
            if not (StringCleaner.isNumeric(submission_id)):
                exc = ResponseException("submission id cannot be parsed into its appropriate type", StatusCode.CLIENT_ERROR)
                return JsonResponse.create(exc, exc.status)

        d1_file = self.jobManager.getDFileForSubmission(submission_id, "d1")
        if d1_file is None:
            response = {"status": "invalid", "url": "", "start": "", "end": "", "message": ""}
            return JsonResponse.create(StatusCode.OK, response)

        status = self.jobManager.getJobStatusNameById(d1_file.status_id)

        if status == "finished" and not d1_file.is_submitted and False: # TODO: Remove "and False" when submission of D1 is desired
            job_id = self.jobManager.getJobBySubmissionFileTypeAndJobType(submission_id, "award_procurement", "file_upload").job_id
            self.jobManager.updateDFileName(submission_id, d1_file.upload_file_name, "award")
            result = self.finalize(jobId=job_id)
            self.jobManager.markDFileAsSubmitted(d1_file.d_file_id)
            if result.status_code != 200:
                raise ResponseException(result.data)

        file_elements = d1_file.upload_file_name.split('/')
        user_id = file_elements[0]
        timestamped_filename = file_elements[-1]

        url = "" if status != "finished" else self.s3manager.getSignedUrl(path=str(user_id),
                                                                          fileName=timestamped_filename, method="GET")
        error_message = "" if d1_file.error_message is None else d1_file.error_message
        start_date = d1_file.start_date.strftime("%m/%d/%Y")
        end_date = d1_file.end_date.strftime("%m/%d/%Y")

        response = {"status": status, "url": url, "start": start_date, "end": end_date,
                    "message": error_message}

        return JsonResponse.create(StatusCode.OK, response)


    def generateD2File(self):
        """ Initiates the generation of D2 """
        requestDict = RequestDictionary(self.request)

        if not (requestDict.exists("submission_id") and requestDict.exists("start") and requestDict.exists("end")):
            exc = ResponseException("Generate D2 Files route requires submission_id, start, and end",
                                    StatusCode.CLIENT_ERROR)
            return JsonResponse.create(exc, exc.status)

        submission_id = requestDict.getValue("submission_id")
        start_date = requestDict.getValue("start")
        end_date = requestDict.getValue("end")

        if not (StringCleaner.isNumeric(submission_id) and StringCleaner.isDate(start_date) and StringCleaner.isDate(
                end_date)):
            exc = ResponseException("submission id, start, and/or end cannot be parsed into their appropriate types",
                                    StatusCode.CLIENT_ERROR)
            return JsonResponse.create(exc, exc.status)

        cgac_code = self.jobManager.getSubmissionById(submission_id).cgac_code
        get_url = CONFIG_BROKER["d2_url"].format(cgac_code, start_date, end_date)

        jq = JobQueue(job_queue_url=CONFIG_JOB_QUEUE['url'])

        # Generate and upload D2 file to S3
        user_id = LoginSession.getName(session)
        timestamped_name = s3UrlHandler.getTimestampedFilename(CONFIG_BROKER["d2_file_name"])
        upload_file_name = "".join([str(user_id), "/", timestamped_name])
        d_file_id = self.jobManager.createDFileMeta(submission_id, start_date, end_date, "d2", CONFIG_BROKER["d2_file_name"], upload_file_name)
        self.jobManager.setDFileStatus(d_file_id, "waiting")
        jq.generate_d_file.delay(get_url, CONFIG_BROKER["d2_file_name"], user_id, d_file_id, InterfaceHolder, timestamped_name, skip_gen=True)

        # Check status for D2 file
        return self.checkD2File()

    def checkD2File(self, submission_id=None):
        """ Check status of D2 file generation """
        if submission_id is None:
            requestDict = RequestDictionary(self.request)
            if not (requestDict.exists("submission_id")):
                exc = ResponseException("Check D2 Files route requires submission_id", StatusCode.CLIENT_ERROR)
                return JsonResponse.create(exc, exc.status)

            submission_id = requestDict.getValue("submission_id")
            if not (StringCleaner.isNumeric(submission_id)):
                exc = ResponseException("submission id cannot be parsed into its appropriate type", StatusCode.CLIENT_ERROR)
                return JsonResponse.create(exc, exc.status)

        d2_file = self.jobManager.getDFileForSubmission(submission_id, "d2")
        if d2_file is None:
            response = {"status": "invalid", "url": "", "start": "", "end": "", "message": ""}
            return JsonResponse.create(StatusCode.OK, response)

        status = self.jobManager.getJobStatusNameById(d2_file.status_id)

        if status == "finished" and not d2_file.is_submitted:
            job_id = self.jobManager.getJobBySubmissionFileTypeAndJobType(submission_id, "award", "file_upload").job_id
            self.jobManager.updateDFileName(submission_id, d2_file.upload_file_name, "award")
            result = self.finalize(jobId=job_id)
            self.jobManager.markDFileAsSubmitted(d2_file.d_file_id)
            if result.status_code != 200:
                raise ResponseException(result.data)

        file_elements = d2_file.upload_file_name.split('/')
        user_id = file_elements[0]
        timestamped_filename = file_elements[-1]

        url = "" if status != "finished" else self.s3manager.getSignedUrl(path=str(user_id), fileName=timestamped_filename, method="GET")

        error_message = "" if d2_file.error_message is None else d2_file.error_message
        start_date = d2_file.start_date.strftime("%m/%d/%Y")
        end_date = d2_file.end_date.strftime("%m/%d/%Y")

        response = {"status": status, "url": url, "start": start_date, "end": end_date,
                    "message": error_message}

        return JsonResponse.create(StatusCode.OK, response)

    def getProtectedFiles(self):
        """ Returns a set of urls to protected files on the help page """
        response = {}
        if self.isLocal:
            response["urls"] = {}
            return JsonResponse.create(StatusCode.CLIENT_ERROR, response)

        response["urls"] = self.s3manager.getFileUrls(bucket_name=CONFIG_BROKER["static_files_bucket"], path=CONFIG_BROKER["help_files_path"])
        return JsonResponse.create(StatusCode.OK, response)
