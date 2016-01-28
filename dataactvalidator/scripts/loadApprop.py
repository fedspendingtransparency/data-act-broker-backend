""" This script loads fields and rules for the appropriations file type """
from dataactvalidator.filestreaming.schemaLoader import SchemaLoader

SchemaLoader.loadFields("appropriations","../tests/appropriationsFields.csv")
SchemaLoader.loadRules("appropriations","../tests/appropriationsRules.csv")
