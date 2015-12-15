""" This script clears all jobs from job_status and job_dependency """
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from dataactcore.models.jobTrackerInterface import JobTrackerInterface

credentialsFile = JobTrackerInterface.getCredFilePath()
dbName = JobTrackerInterface.getDbName()

# Load credentials from config file
cred = open(credentialsFile,"r").read()
credDict = json.loads(cred)

# Create engine and session
engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

# Create tables
sqlStatements = ["DELETE FROM job_dependency", "DELETE FROM job_status", "DELETE FROM submission"]

for statement in sqlStatements:
    try:
        connection.execute(statement)
    except ProgrammingError as e:
        # Usually a table exists error, print and continue
        print(e.message)
