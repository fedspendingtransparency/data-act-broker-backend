import os
import os.path
import argparse
import dataactvalidator
from dataactvalidator.scripts.setupValidationDB import setupValidationDB
from dataactvalidator.scripts.setupStagingDB import setupStagingDB
from dataactvalidator.scripts.setupTASIndexs import setupTASIndexs
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader
from dataactvalidator.filestreaming.tasLoader import TASLoader


def baseScript():
    if os.getuid() != 0:
        print ("Please run this script with sudo")

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
    loadTas()
    print ("The validator has been initialized. You may now run the validator with the -start argument.")


def loadValidator():
    """Load validator fields and rules from config."""
    # TODO: better place to stash the validator config path
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


def loadTas():
    """Load validator fields and rules from config."""
    # TODO: better place to stash the validator config path
    validator_config_path = os.path.join(
        os.path.dirname(dataactvalidator.__file__), "config")
    tas = os.path.join(validator_config_path, "all_tas_betc.csv")
    try:
        TASLoader.loadFields(tas)
        setupTASIndexs()
    except IOError:
        print("Can't open file: {}".format(tas))


def setupDB():
    setupValidationDB(True)
    setupStagingDB()


def start():
    from dataactvalidator.app import runApp
    runApp()

if __name__ == '__main__':
    options()
