import sqlalchemy
import sys
import os
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

try:
    from jobModels import JobStatus,JobDependency,Status,Type,Resource
except Exception:
    # TODO add models directory to path with relative path
    #sys.path.append(os.getcwd()+"/../models/")
    sys.path.append("c:/Users/jworcest/Documents/data-act-broker/app/models")
    from jobModels import JobStatus,JobDependency,Status,Type,Resource

class JobHandler:
    dbName = "job_tracker"
    credentialsFile = "dbCred.json"
    host = "localhost"
    port = "5432"
    # Available instance variables:  session, waitingStatus, runningStatus, fileUploadType, dbUploadType, validationType, externalValidationTYpe

    def __init__(self):
        print("Creating Job Handler")
        # Load credentials from config file
        cred = open(self.credentialsFile,"r").read()
        credDict = json.loads(cred)
        # Get status and type values from queries
        engine = create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+self.host+":"+self.port+"/"+self.dbName)
        engine.connect()
        Session = sessionmaker(bind=engine)
        self.session = Session()
        # Set up instance variables for status and type values
        self.waitingStatus = self.setStatus("waiting")
        self.runningStatus = self.setStatus("running")
        self.fileUploadType = self.setType("file_upload")
        self.dbUploadType = self.setType("db_upload")
        self.validationType = self.setType("validation")
        self.externalValidationType = self.setType("external_validation")

    # Get a result for specified status, if not unique throw an exception
    def setStatus(self,name):
        queryResult = self.session.query(Status.status_id).filter(Status.name==name).all()
        if(len(queryResult) != 1):
            # Did not get a unique result
            raise ValueError("Database does not contain a unique ID for status "+name)
        else:
            return queryResult[0].status_id

    # Get a result for specified type, if not unique throw an exception
    def setType(self,name):
        queryResult = self.session.query(Type.type_id).filter(Type.name==name).all()
        if(len(queryResult) != 1):
            # Did not get a unique result
            raise ValueError("Database does not contain a unique ID for type "+name)
        else:
            return queryResult[0].type_id

    # Given the filenames to be uploaded, create the set of jobs needing to be completed for this submission
    def createJobs(self,filenames):
        # Try a basic add first

        #TODO for missing files, create a job specifying what we need to pull out of the production database to validate against
        for filename in filenames:
            # Create upload job, mark as running since frontend should be doing this upload
            print("Trying to add these values:")
            print(filename)
            print(self.runningStatus)
            print(self.fileUploadType)
            fileJob = JobStatus(filename = filename, status_id = self.runningStatus, type_id = self.fileUploadType, resource_id = 0)
            #self.session.begin()

            self.session.add(fileJob)
            # Create parse into DB job
            dbJob = JobStatus(filename = filename, status_id = self.waitingStatus, type_id = self.dbUploadType, resource_id = 0)
            self.session.add(dbJob)

        # Create validation job
        validationJob = JobStatus(status_id = self.waitingStatus, type_id = self.validationType, resource_id = 0)
        self.session.add(validationJob)
        # Create external validation job
        externalJob = JobStatus(status_id = self.waitingStatus, type_id = self.externalValidationType, resource_id = 0)
        self.session.add(externalJob)
        # Create dependencies, how do we get job id's back?

        # Commit all changes
        self.session.commit()
        return True