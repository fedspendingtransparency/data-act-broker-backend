from dataactcore.scripts.configure import ConfigureCore
from dataactbroker.scripts.configure import ConfigureBroker
from dataactbroker.scripts.setupEmails import setupEmails
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.scripts.setupJobTrackerDB import setupJobTrackerDB
from dataactcore.scripts.setupErrorDB import setupErrorDB
from dataactcore.scripts.setupUserDB import setupUserDB
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
	optionsDict = vars(args)

	noArgs = True
	for arg in optionsDict:
		if(optionsDict[arg]):
			noArgs = False
			globals()[arg]()

	if(noArgs):
		print ("Please enter an argument.")


def initialize():
	# print "in initialize"
	configAWS()
	configDB()
	configureBroker()
	setupDB()
	print ("The broker has been initialized. You may now run the broker with the -start argument.")

def configureBroker():
	ConfigureBroker.promptBroker()

def configAWS():
	ConfigureCore.promptS3()
	ConfigureCore.promptLogging()

def configDB():
	ConfigureCore.promptDatabase()

def setupDB():
	setupJobTrackerDB(hardReset=True)
	setupErrorDB(True)
	setupUserDB(True)
	setupEmails()

def start():
	from dataactbroker.app import runApp
	runApp()

if __name__ == '__main__':
	options()
