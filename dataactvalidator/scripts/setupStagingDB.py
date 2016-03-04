""" Create the staging database """

from dataactcore.scripts.databaseSetup import runCommands
from dataactvalidator.interfaces.validatorStagingInterface import ValidatorStagingInterface

def setupStaging():
    runCommands(ValidatorStagingInterface.getCredDict(),[],"staging")

if __name__ == '__main__':
    setupStaging()
