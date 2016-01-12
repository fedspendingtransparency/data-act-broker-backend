import sqlalchemy
from sqlalchemy.exc import ProgrammingError, IntegrityError

def runCommands(credDict, sqlCommands, dbName, connection = None):
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
        print(e.message)
    if(connection == None):
        engine = sqlalchemy.create_engine("postgresql://"+credDict["username"]+":"+credDict["password"]+"@"+credDict["host"]+":"+credDict["port"]+"/"+dbName)
        connection = engine.connect()
    for statement in sqlCommands:
        #print("Execute statement: " + statement)
        try:
            connection.execute(statement)
        except (ProgrammingError, IntegrityError) as e:
            # Usually a table exists error, print and continue
            print(e.message)
    connection.close()
