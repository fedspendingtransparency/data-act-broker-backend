from dataactcore.scripts.configure import ConfigureCore
from dataactvalidator.scripts.configure import ConfigureValidator
from dataactcore.utils.jsonResponse import JsonResponse
import sys
import os


def baseScript():
	ConfigureCore.promtS3()
	ConfigureCore.promtDatabase()
	from dataactcore.scripts.createJobTables import createJobTables
	from dataactcore.scripts.setupErrorDB import setupErrorDB
	from dataactcore.scripts.setupValidationDB import setupValidationDB
	import dataactcore.scripts.setupStaging
	createJobTables()
	ConfigureValidator.promptWebservice()
	setupValidationDB(True)
	setupErrorDB(True)
	dataactcore.scripts.setupStaging
	from dataactvalidator.app import runApp
	runApp()

if __name__ == '__main__':
    baseScript()

