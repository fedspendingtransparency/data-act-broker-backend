from dataactcore.scripts.configure import ConfigureCore
from dataactbroker.scripts.configure import ConfigureBroker
from dataactcore.utils.jsonResponse import JsonResponse
import sys
import os
import argparse

def options():
	if ( os.getuid() != 0) :
		print ("Please run this script with sudo")

	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--initialize", action="store_true",help="Runs all of the setup options")
	parser.add_argument("-c", "--configureBroker", action="store_true",help="Configures broker settings" )
	parser.add_argument("-cdb", "--configDB", action="store_true",help="Configures database connection" )
	parser.add_argument("-aws","--configAWS", action="store_true",help="Configures AWS settings" )
	parser.add_argument("-db", "--setupDB", action="store_true",help="Creates the database schema" )
	parser.add_argument("-s", "--start", action="store_true",help="Starts the broker" )
	args = parser.parse_args()

	if(len(sys.argv) > 1):
		if args.initialize:
			initialize()
		if args.ConfigureBroker:
			configValidator()
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
	configBroker()
	resetDB()
	print ("The broker has been initialized. You may now run the broker with the -start argument.")

def configBroker():
	ConfigureBroker.promptBroker()

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

def startBroker():
	from dataactvalidator.app import runApp
	runApp()

if __name__ == '__main__':
    options()
