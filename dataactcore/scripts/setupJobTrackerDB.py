""" This script creates the needed tables for the job tracker database.  Postgres, sqlalchemy, and psycopg2 must be installed,
 and username and password must be in a json file called dbCred.json

 """

from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.userInterface import UserInterface

def setupJobTrackerDB(connection = None, hardReset = False):
    if(hardReset):
        sql = ["DROP TABLE IF EXISTS job_dependency",
                "DROP TABLE IF EXISTS job_status",
                "DROP TABLE IF EXISTS file_type",
                "DROP TABLE IF EXISTS submission",
                "DROP TABLE IF EXISTS resource",
                "DROP TABLE IF EXISTS resource_status",
                "DROP TABLE IF EXISTS type",
                "DROP TABLE IF EXISTS status",
                "DROP SEQUENCE IF EXISTS resourceIdSerial",
                "DROP SEQUENCE IF EXISTS submissionIdSerial",
                "DROP SEQUENCE IF EXISTS fileTypeSerial",
                "DROP SEQUENCE IF EXISTS jobIdSerial",
                "DROP SEQUENCE IF EXISTS dependencyIdSerial"]
        runCommands(JobTrackerInterface.getCredDict(),sql,"job_tracker",connection)

    sql = ["CREATE TABLE status (status_id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL)",
                "CREATE TABLE type (type_id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL)",
                "CREATE SEQUENCE resourceIdSerial START 1",
                "CREATE TABLE resource_status (resource_status_id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL)",
                "CREATE TABLE resource (resource_id integer PRIMARY KEY DEFAULT nextval('resourceIdSerial'), IP text NOT NULL, status_id integer NOT NULL REFERENCES resource_status)",
                "CREATE SEQUENCE submissionIdSerial START 1",
                "CREATE TABLE submission (submission_id integer PRIMARY KEY DEFAULT nextval('submissionIdSerial'), datetime_utc text)",
                "CREATE SEQUENCE fileTypeSerial START 1",
                "CREATE TABLE file_type (file_type_id integer PRIMARY KEY DEFAULT nextval('fileTypeSerial'), name text, description text)",
                "CREATE SEQUENCE jobIdSerial START 1",
                "CREATE TABLE job_status (job_id integer PRIMARY KEY DEFAULT nextval('jobIdSerial'), filename text, file_type_id integer REFERENCES file_type, status_id integer NOT NULL REFERENCES status, type_id integer NOT NULL REFERENCES type, resource_id integer REFERENCES resource, submission_id integer NOT NULL REFERENCES submission, staging_table text)",
                "CREATE SEQUENCE dependencyIdSerial START 1",
                "CREATE TABLE job_dependency (dependency_id integer PRIMARY KEY DEFAULT nextval('dependencyIdSerial'), job_id integer NOT NULL REFERENCES job_status, prerequisite_id integer NOT NULL REFERENCES job_status)",
                "INSERT INTO resource_status (resource_status_id, name, description) VALUES (1,'ready','available for new jobs'), (2,'running','currently working on a job'), (3,'unresponsive','resource seems to be unresponsive')",
                "INSERT INTO status (status_id,name, description) VALUES (1, 'waiting', 'check dependency table'), (2, 'ready', 'can be assigned'), (3, 'running', 'job is currently in progress'), (4, 'finished', 'job is complete'),(5, 'invalid', 'job is invalid'),(6, 'failed', 'job failed to complete')",
                "INSERT INTO type (type_id,name,description) VALUES (1, 'file_upload', 'file must be uploaded to S3'), (2, 'csv_record_validation', 'do record level validation and add to staging DB'), (3, 'db_transfer', 'information must be moved from production DB to staging DB'), (4, 'validation', 'new information must be validated'), (5, 'external_validation', 'new information must be validated against external sources')",
                "INSERT INTO file_type (file_type_id, name, description) VALUES (1, 'award', ''), (2, 'award_financial', ''), (3, 'appropriations', ''), (4, 'procurement', '')",
                ]
    runCommands(JobTrackerInterface.getCredDict(),sql,"job_tracker",connection)

if __name__ == '__main__':
    setupJobTrackerDB(hardReset = True)
