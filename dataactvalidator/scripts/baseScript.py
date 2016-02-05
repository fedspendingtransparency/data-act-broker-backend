import sys
import os
import argparse
from dataactcore.scripts.configure import ConfigureCore
from dataactvalidator.scripts.configure import ConfigureValidator

def baseScript():
	if ( os.getuid() != 0) :
		print ("Please run this script with sudo")

	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--initialize", action="store_true",help="Runs all of the setup options")
	parser.add_argument("-c", "--configValidator", action="store_true",help="Configures validator settings" )
	parser.add_argument("-l", "--loadValidator", action="store_true",help="Loads validator Schema files and rules" )
	parser.add_argument("-cdb", "--configDB", action="store_true",help="Configures database connection" )
	parser.add_argument("-aws","--configAWS", action="store_true",help="Configures AWS settings" )
	parser.add_argument("-db", "--setupDB", action="store_true",help="Creates the database schema" )
	parser.add_argument("-s", "--start", action="store_true",help="Starts the validator" )
	args = parser.parse_args()

	if(len(sys.argv) > 1):
		if args.initialize:
			initialize()
		if args.configValidator:
			configValidator()
		if args.loadValidator:
			loadValidator()
		if args.configAWS:
			configAWS()
		if args.configDB:
			configDB()
		if args.setupDB:
			resetDB()
		if args.start:
			startValidator()
	else:
		print ("Please enter an argument.")


def initialize():
	# print "in initialize"
	configAWS()
	configDB()
	configValidator()
	resetDB()
	loadValidator()
	print ("The validator has been initialized. You may now run the validator with the -start argument.")

def configValidator():
	ConfigureValidator.promptWebservice()

def loadValidator():
	ConfigureValidator.promptForAppropriations()
	ConfigureValidator.promptForTAS()

def configAWS():
	ConfigureCore.promptS3()

def configDB():
	ConfigureCore.promptDatabase()

def resetDB():
	from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
	from dataactcore.scripts.setupErrorDB import setupErrorDB
	from dataactcore.scripts.setupValidationDB import setupValidationDB
	import dataactcore.scripts.setupStaging
	setupJobTrackerDB(hardReset=True)
	setupValidationDB(True)
	setupErrorDB(True)
	dataactcore.scripts.setupStaging

def startValidator():
	from dataactvalidator.app import runApp
	runApp()

if __name__ == '__main__':
    baseScript()
