import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, IntegrityError

def runCommands(credDict, sqlCommands,dbName) :
    dbBaseName = "postgres"
    baseEngine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbBaseName, isolation_level = "AUTOCOMMIT")
    try:
        baseEngine.connect().execute("CREATE DATABASE " + '"' + dbName + '"')

    except ProgrammingError as e:
        # Happens if DB exists, just print and carry on
        raise e
        print(e.message)
    engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
    connection = engine.connect()
    for statement in sqlCommands:
        try:
            connection.execute(statement)
        except (ProgrammingError, IntegrityError) as e:
            # Usually a table exists error, print and continue
            print(e.message)
