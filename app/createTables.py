""" This script creates the needed tables for the job tracker database.  Postgres, sqlalchemy, and psycopg2 must be installed,
 and username and password must be in a json file called dbCred.json

 """
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, IntegrityError

credentialsFile = "dbCred.json"
userDbName = "user_manager"
dbBaseName = "postgres"
dbName = "job_tracker"

# Load credentials from config file
cred = open(credentialsFile,"r").read()
credDict = json.loads(cred)

# Create database by connecting to default postgres database
baseEngine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbBaseName, isolation_level = "AUTOCOMMIT")
try:

    baseEngine.connect().execute("CREATE DATABASE " + '"' + dbName + '"')

except ProgrammingError as e:
    # Happens if DB exists, just print and carry on
    print(e.message)

try:
    baseEngine.connect().execute("CREATE DATABASE " + '"' + userDbName + '"')
except ProgrammingError as e:
    # Happens if DB exists, just print and carry on
    print(e.message)

# Create engine and session
engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
userEngine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+userDbName)

connection = engine.connect()
userConnection = userEngine.connect()

Session = sessionmaker(bind=engine)
session = Session()
UserSessionBase = sessionmaker(bind=userEngine)
userSession = UserSessionBase()

# Create tables
sqlStatements = ["CREATE SEQUENCE jobIdSerial START 1",
                 "CREATE TABLE job_status (job_id integer PRIMARY KEY DEFAULT nextval('jobIdSerial'), filename text, status_id integer NOT NULL, type_id integer NOT NULL, resource_id integer NOT NULL)",
                 "CREATE SEQUENCE dependencyIdSerial START 1",
                 "CREATE TABLE job_dependency (dependency_id integer PRIMARY KEY DEFAULT nextval('dependencyIdSerial'), job_id integer NOT NULL, prerequisite_id integer NOT NULL)",
                 "CREATE TABLE status (status_id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL)",
                 "CREATE TABLE type (type_id integer PRIMARY KEY, name text NOT NULL, description text NOT NULL)",
                 "CREATE SEQUENCE resourceIdSerial START 1",
                 "CREATE TABLE resource (resource_id integer PRIMARY KEY DEFAULT nextval('resourceIdSerial'))",
                 "INSERT INTO status (status_id,name, description) VALUES (1, 'waiting', 'check dependency table'), (2, 'ready', 'can be assigned'), (3, 'running', 'job is currently in progress'), (4, 'finished', 'job is complete')",
                 "INSERT INTO type (type_id,name,description) VALUES (1, 'file_upload', 'file must be uploaded to S3'), (2, 'csv_record_validation', 'do record level validation and add to staging DB'), (3, 'db_transfer', 'information must be moved from production DB to staging DB'), (4, 'validation', 'new information must be validated'), (5, 'external_validation', 'new information must be validated against external sources')"]
for statement in sqlStatements:
    try:
        connection.execute(statement)
    except (ProgrammingError, IntegrityError) as e:
        # Usually a table exists error, print and continue
        print(e.message)

userStatements = ["CREATE SEQUENCE userIdSerial START 1",
                  "CREATE TABLE users (user_id integer PRIMARY KEY DEFAULT nextval('userIdSerial'), username text)"]
for statement in userStatements:
    try:
        userConnection.execute(statement)
    except ProgrammingError as e:
        # Usually a table exists error, print and continue
        print(e.message)
