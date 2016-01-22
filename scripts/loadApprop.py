from filestreaming.schemaLoader import SchemaLoader

SchemaLoader.loadFields("appropriations","../tests/appropriationsFields.csv")
SchemaLoader.loadRules("appropriations","../tests/appropriationsRules.csv")