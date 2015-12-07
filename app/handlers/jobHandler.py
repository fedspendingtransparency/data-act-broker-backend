import sqlalchemy
import sys
import os
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, update
from models.jobModels import JobStatus,JobDependency,Status,Type,Resource


class JobHandler:
    """ Responsible for all interaction with the job tracker database

    Static fields:
    DB_NAME -- Name of Postgres job tracker database
    CREDENTIALS_FILE -- File that holds a JSON with keys "username" and "password"
    HOST -- host where database is located
    PORT -- port for connecting to database

    Instance fields:
    engine -- sqlalchemy engine for generating connections and sessions
    connection -- sqlalchemy connection for executing direct SQL statements
    session -- sqlalchemy session for ORM usage
    waitingStatus -- status_id for "waiting"
    runningStatus -- status_id for "running"
    finishedStatus -- status_id for "finished"
    fileUploadType -- type_id for "file_upload"
    dbUploadType -- type_id for "db_upload"
    validationType -- type_id for "validation"
    externalValidationType -- type_id for "external_validation"
    """
    DB_NAME = "job_tracker"
    CREDENTIALS_FILE = "dbCred.json"
    # Available instance variables:  session, waitingStatus, runningStatus, fileUploadType, dbUploadType, validationType, externalValidationTYpe

    def __init__(self):
        """ Sets up connection to job_tracker database """

        # Load credentials from config file
        cred = open(self.CREDENTIALS_FILE, "r").read()
        credDict = json.loads(cred)
        # Get status and type values from queries
        self.engine = create_engine("postgresql://" + credDict["username"] +":" + credDict["password"] +"@" +credDict["host"] + ":" + credDict["port"] + "/" + self.DB_NAME)
        self.connection = self.engine.connect()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        # Set up instance variables for status and type values
        self.waitingStatus = self.setStatus("waiting")
        self.runningStatus = self.setStatus("running")
        self.finishedStatus = self.setStatus("finished")
        self.fileUploadType = self.setType("file_upload")
        self.dbUploadType = self.setType("db_upload")
        self.validationType = self.setType("validation")
        self.externalValidationType = self.setType("external_validation")

    def setStatus(self,name):
        """  Get an id for specified status, if not unique throw an exception

        Arguments:
        name -- Name of status to get an id for

        Returns:
        status_id of the specified status
        """
        queryResult = self.session.query(Status.status_id).filter(Status.name==name).all()
        if(len(queryResult) != 1):
            # Did not get a unique result
            raise ValueError("Database does not contain a unique ID for status "+name)
        else:
            return queryResult[0].status_id

    # Get a result for specified type, if not unique throw an exception
    def setType(self,name):
        """  Get an id for specified type, if not unique throw an exception

        Arguments:
        name -- Name of type to get an id for

        Returns:
        type_id of the specified type
        """
        queryResult = self.session.query(Type.type_id).filter(Type.name==name).all()
        if(len(queryResult) != 1):
            # Did not get a unique result
            raise ValueError("Database does not contain a unique ID for type "+name)
        else:
            return queryResult[0].type_id

    def createJobs(self,filenames):
        """  Given the filenames to be uploaded, create the set of jobs needing to be completed for this submission

        Arguments:
        filenames -- List of filenames to be uploaded

        Returns:
        Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """

        jobsRequired, uploadDict = self.addUploadJobs(filenames)

        # Create validation job
        validationJob = JobStatus(status_id = self.waitingStatus, type_id = self.validationType, resource_id = 0)
        self.session.add(validationJob)
        # Create external validation job
        externalJob = JobStatus(status_id = self.waitingStatus, type_id = self.externalValidationType, resource_id = 0)
        self.session.add(externalJob)
        self.session.flush()
        # Create dependencies for validation jobs
        for job_id in jobsRequired:
            valDependency = JobDependency(job_id = validationJob.job_id, prerequisite_id = job_id)
            self.session.add(valDependency)
            extDependency = JobDependency(job_id = externalJob.job_id, prerequisite_id = job_id)
            self.session.add(extDependency)

        # Commit all changes
        self.session.commit()
        return uploadDict

    def addUploadJobs(self,filenames):
        """  Add upload jobs to job tracker database

        Arguments:
        filenames -- List of filenames to be uploaded

        Returns:
        jobsRequired -- List of job ids required for validation jobs, used to populate the prerequisite table
        uploadDict -- Dictionary of upload ids by filename to return to client, used for calling finalize_submission route
        """

        # Keep list of job ids required for validation jobs
        jobsRequired = []
        # Dictionary of upload ids by filename to return to client
        uploadDict = {}

        for originalName, filename in filenames:
            # Create upload job, mark as running since frontend should be doing this upload
            fileJob = JobStatus(filename = filename, status_id = self.runningStatus, type_id = self.fileUploadType, resource_id = 0)

            self.session.add(fileJob)

            # Create parse into DB job
            dbJob = JobStatus(filename = filename, status_id = self.waitingStatus, type_id = self.dbUploadType, resource_id = 0)
            self.session.add(dbJob)
            self.session.flush()
            # Add dependency between file upload and db upload
            uploadDependency = JobDependency(job_id = dbJob.job_id, prerequisite_id = fileJob.job_id)
            self.session.add(uploadDependency)
            # Add both jobs to required list
            jobsRequired.append(fileJob.job_id)
            jobsRequired.append(dbJob.job_id)
            uploadDict[originalName] = fileJob.job_id

        # Return list of upload jobs
        return jobsRequired, uploadDict

    def changeToFinished(self, jobId):
        """  Mark an upload job as finished

        Arguments:
        jobId -- job_id to mark as finished

        """

        # Pull from job status table
        queryResult = self.session.query(JobStatus).filter(JobStatus.job_id == jobId).all()
        if(len(queryResult) != 1):
            # Did not find a unique match to job ID
            raise ValueError("Job ID not found")
        jobToChange = queryResult[0]
        # Change status to finished
        jobToChange.status_id = self.finishedStatus
        # Commit changes
        self.session.commit()
