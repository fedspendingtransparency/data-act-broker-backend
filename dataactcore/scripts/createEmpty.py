from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.baseInterface import BaseInterface

def createEmptyDatabase():
    # Create empty databases, can be used before running alembic migration for first time
    dbNames = ["error_data","job_tracker","staging","user_manager","validation"]
    for dbName in dbNames:
        runCommands(BaseInterface.getCredDict(),[],dbName)

if __name__ == "__main__":
    createEmptyDatabase()