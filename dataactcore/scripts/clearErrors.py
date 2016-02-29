""" This script clears all jobs from job_status and job_dependency """
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from dataactcore.models.errorInterface import ErrorInterface
from dataactcore.models.baseInterface import BaseInterface

def clearErrors():
    credentialsFile = ErrorInterface.getCredFilePath()
    dbName = ErrorInterface.getDbName()

    # Load credentials from config file
    cred = open(credentialsFile,"r").read()
    credDict = json.loads(cred)

    # Create engine and session
    engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
    connection = engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create tables
    sqlStatements = ["DDELETE FROM error_data", "DDELETE FROM file_status"]

    for statement in sqlStatements:
        try:
            connection.execute(statement)
        except ProgrammingError as e:
            # Usually a table exists error, print and continue
            BaseInterface.logDbError(e)

if __name__ == '__main__':
    clearErrors()
