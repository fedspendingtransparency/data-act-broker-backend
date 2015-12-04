# This script clears all jobs from job_status and job_dependency
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError

credentialsFile = "dbCred.json"
host = "localhost"
port = "5432"
dbName = "job_tracker"
dbBaseName = "postgres"

# Load credentials from config file
cred = open(credentialsFile,"r").read()
credDict = json.loads(cred)

# Create engine and session
engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+host+":"+port+"/"+dbName)
connection = engine.connect()
Session = sessionmaker(bind=engine)
session = Session()

# TODO: refactor this to use sqlalchemy methods rather than direct SQL statements
# Create tables
sqlStatements = ["DELETE FROM job_status", "DELETE FROM job_dependency"]

for statement in sqlStatements:
    try:
        connection.execute(statement)
    except ProgrammingError as e:
        # Usually a table exists error, print and continue
        print(e.message)