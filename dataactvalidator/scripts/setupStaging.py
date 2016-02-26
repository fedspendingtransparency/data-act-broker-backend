""" Create the staging database """

from dataactcore.scripts.databaseSetup import runCommands
from dataactvalidator.interfaces.validatorStagingInterface import ValidatorStagingInterface

runCommands(ValidatorStagingInterface.getCredDict(),[],"staging")
