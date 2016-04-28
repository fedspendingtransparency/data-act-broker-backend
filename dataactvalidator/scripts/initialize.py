import os
import os.path
import argparse
import dataactvalidator
from dataactvalidator.scripts.setupValidationDB import setupValidationDB
from dataactvalidator.scripts.setupStagingDB import setupStagingDB
from dataactvalidator.scripts.loadTas import loadTas
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader


def options():

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--initialize", action="store_true", help="Runs all of the setup options")
    parser.add_argument("-l", "--loadValidator", action="store_true", help="Loads validator Schema files and rules")
    parser.add_argument("-t", "--loadTas", action="store_true", help="Loads valid TAS combinations from Central Accounting Reporting Systems (CARS)")
    parser.add_argument("-db", "--setupDB", action="store_true", help="Creates the validator database schema")
    parser.add_argument("-s", "--start", action="store_true", help="Starts the validator")
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
    print ("Setting up validator databases...")
    setupDB()
    print ("Loading validator fields and rules...")
    loadValidator()
    print ("Loading TAS file...")
    validator_config_path = os.path.join(
    os.path.dirname(dataactvalidator.__file__), "config")
    tas = os.path.join(validator_config_path, "all_tas_betc.csv")
    loadTas(tas)
    print ("The validator has been initialized. You may now run the validator with the -start argument.")


def loadValidator():
    """Load validator fields and rules from config."""
    validator_config_path = os.path.join(
    os.path.dirname(dataactvalidator.__file__), "config")
    appropriationsFields = os.path.join(validator_config_path, "appropFields.csv")
    try:
        SchemaLoader.loadFields("appropriations", appropriationsFields)
    except IOError:
        print("Can't open file: {}".format(appropriationsFields))


    appropriationsRules = os.path.join(validator_config_path, "appropRules.csv")
    try:
        SchemaLoader.loadRules("appropriations", appropriationsRules)
    except IOError:
        print("Can't open file: {}".format(appropriationsRules))


def setupDB():
    setupValidationDB()
    setupStagingDB()


def start():
    from dataactvalidator.app import runApp
    runApp()

if __name__ == '__main__':
    options()
