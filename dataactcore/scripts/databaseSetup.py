import sqlalchemy
from sqlalchemy.exc import ProgrammingError, IntegrityError
from dataactcore.models.baseInterface import BaseInterface

def runCommands(credDict, sqlCommands, dbName, connection = None):
    """ Apply commands to specified database """
    dbBaseName = "postgres"
    baseEngine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbBaseName, isolation_level = "AUTOCOMMIT")

    try:
        connect = baseEngine.connect()
        rows  = connect.execute("SELECT 1 FROM pg_database WHERE datname = '" +dbName+"'")
        if ((rows.rowcount) == 0) :
            connect.execute("CREATE DATABASE " + '"' + dbName + '"')
        connect.close()
    except ProgrammingError as e:
        # Happens if DB exists, just print and carry on
        BaseInterface.logDbError(e)

    if(connection == None):
        engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
        connection = engine.connect()
    for statement in sqlCommands:
        #print("Execute statement: " + statement)
        try:
            connection.execute(statement)
        except (ProgrammingError, IntegrityError) as e:
            # Usually a table exists error, print and continue
            BaseInterface.logDbError(e)
    connection.close()
