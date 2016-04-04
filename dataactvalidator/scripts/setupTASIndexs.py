from dataactcore.scripts.databaseSetup import runCommands
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

def setupTASIndexs():
    """
        Creates indexes for the tas_lookup tables
        Only call this after the table is fully populated
    """
    sql=[
        "CREATE INDEX  ON tas_lookup (allocation_transfer_agency);",
        "CREATE INDEX  ON tas_lookup (agency_identifier);",
        "CREATE INDEX  ON tas_lookup (beginning_period_of_availability);",
        "CREATE INDEX  ON tas_lookup (ending_period_of_availability);",
        "CREATE INDEX  ON tas_lookup (availability_type_code);",
        "CREATE INDEX  ON tas_lookup (main_account_code);",
        "CREATE INDEX  ON tas_lookup (sub_account_code);"
        ]
    runCommands(ValidatorValidationInterface.getCredDict(),sql,"validation")

if __name__ == '__main__':
    setupTASIndexs()
