from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface

def setupTASIndexs():
    """Create tas_lookup indices. Don't run until table is populated."""
    # TODO: define/enable/disable indices via ORM
    validatorDb = ValidatorValidationInterface()
    connection = validatorDb.engine.connect()
    sql=[
        "CREATE INDEX  ON tas_lookup (allocation_transfer_agency);",
        "CREATE INDEX  ON tas_lookup (agency_identifier);",
        "CREATE INDEX  ON tas_lookup (beginning_period_of_availability);",
        "CREATE INDEX  ON tas_lookup (ending_period_of_availability);",
        "CREATE INDEX  ON tas_lookup (availability_type_code);",
        "CREATE INDEX  ON tas_lookup (main_account_code);",
        "CREATE INDEX  ON tas_lookup (sub_account_code);"
        ]
    for s in sql:
        connection.execute(s)
    connection.close()

if __name__ == '__main__':
    setupTASIndexs()
