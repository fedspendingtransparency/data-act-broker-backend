from dataactcore.scripts.configure import ConfigureCore
from dataactvalidator.scripts.configure import ConfigureValidator
from dataactcore.utils.jsonResponse import JsonResponse
import sys
import os


def baseScript():
# for arg in sys.argv:
# 	print arg
	if len(sys.argv) > 1:
		if "-initialize" in sys.argv:
			initialize()
		if "-configValidator" in sys.argv:
			configValidator()
		if "-configAWS" in sys.argv:
			configAWS()
		if "-configDB" in sys.argv:
			configDB()
		if "-resetDB" in sys.argv:
			resetDB()
		if "-start" in sys.argv:
			startValidator()
	else:
		


def initialize():
	# print "in initialize"
	configAWS()
	configDB()
	configValidator()
	resetDB()
	print "The validator has been initialized. You may now run the validator with the -start argument."

def configValidator():
	ConfigureValidator.promptWebservice()

def configAWS():
	ConfigureCore.promtS3()

def configDB():
	ConfigureCore.promtDatabase()

def resetDB():
	from dataactcore.scripts.createJobTables import createJobTables
	from dataactcore.scripts.setupErrorDB import setupErrorDB
	from dataactcore.scripts.setupValidationDB import setupValidationDB
	import dataactcore.scripts.setupStaging
	createJobTables()
	setupValidationDB(True)
	setupErrorDB(True)
	dataactcore.scripts.setupStaging

def startValidator():
	from dataactvalidator.app import runApp
	runApp()

if __name__ == '__main__':
    baseScript()

