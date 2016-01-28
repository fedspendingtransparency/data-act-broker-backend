""" Create the staging database """

from dataactcore.scripts.databaseSetup import runCommands
from dataactcore.models.stagingInterface import StagingInterface

runCommands(StagingInterface.getCredDict(),[],"staging")
