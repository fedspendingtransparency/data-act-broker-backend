import unittest
from decimal import *

from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.validationModels import FieldType, FileColumn
from dataactvalidator.validation_handlers.validator import Validator
from baseTestValidator import BaseTestValidator


class ValidatorTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """Set up class-wide resources (test data)"""
        super(ValidatorTests, cls).setUpClass()
        #TODO: refactor into a pytest fixture

        # create test schema
        stringType = FieldType()
        stringType.field_type_id = 1
        stringType.name = "STRING"

        intType = FieldType()
        intType.field_type_id = 2
        intType.name = "INT"

        floatType = FieldType()
        floatType.field_type_id = 3
        floatType.name = "DECIMAL"

        booleanType = FieldType()
        booleanType.field_type_id = 4
        booleanType.name = "BOOLEAN"

        longType = FieldType()
        longType.field_type_id = 5
        longType.name = "LONG"

        column1 = FileColumn()
        column1.file_column_id = 1
        column1.name = "test1"
        column1.required = True
        column1.field_type = stringType
        column1.file_id = 1
        cls.interfaces.validationDb.populateFile(column1)

        column2 = FileColumn()
        column2.file_column_id = 2
        column2.name = "test2"
        column2.required = True
        column2.field_type = floatType
        column2.file_id = 1
        cls.interfaces.validationDb.populateFile(column2)

        column3 = FileColumn()
        column3.file_column_id = 3
        column3.name = "test3"
        column3.required = True
        column3.field_type = booleanType
        column3.file_id = 1
        cls.interfaces.validationDb.populateFile(column3)

        column4 = FileColumn()
        column4.file_column_id = 3
        column4.name = "test4"
        column4.required = True
        column4.field_type = intType
        column4.file_id = 1
        cls.interfaces.validationDb.populateFile(column4)

        column5 = FileColumn()
        column5.file_column_id = 3
        column5.name = "test5"
        column5.required = False
        column5.field_type = intType
        column5.file_id = 1
        cls.interfaces.validationDb.populateFile(column5)

        column6 = FileColumn()
        column6.file_column_id = 6
        column6.name = "test6"
        column6.required = False
        column6.field_type = stringType
        column6.file_id = 1
        cls.interfaces.validationDb.populateFile(column6)

        column7 = FileColumn()
        column7.file_column_id = 7
        column7.name = "test7"
        column7.required = False
        column7.field_type = longType
        column7.file_id = 1
        cls.interfaces.validationDb.populateFile(column7)

        cls.schema = {
            "test1": column1,
            "test2": column2,
            "test3": column3,
            "test4": column4,
            "test5": column5,
            "test6": column6,
            "test7": column7
        }

    def test_types(self):
        """Test data type checks."""
        self.assertTrue(Validator.checkType("1234Test", "STRING"))
        self.assertFalse(Validator.checkType("1234Test", "INT"))
        self.assertFalse(Validator.checkType("1234Test", "DECIMAL"))
        self.assertFalse(Validator.checkType("1234Test", "BOOLEAN"))
        self.assertFalse(Validator.checkType("1234Test", "LONG"))

        self.assertTrue(Validator.checkType("", "STRING"))
        self.assertTrue(Validator.checkType("", "INT"))
        self.assertTrue(Validator.checkType("", "DECIMAL"))
        self.assertTrue(Validator.checkType("", "BOOLEAN"))
        self.assertTrue(Validator.checkType("", "LONG"))

        self.assertTrue(Validator.checkType("01234", "STRING"))
        self.assertTrue(Validator.checkType("01234", "INT"))
        self.assertTrue(Validator.checkType("01234", "DECIMAL"))
        self.assertTrue(Validator.checkType("01234", "LONG"))
        self.assertFalse(Validator.checkType("01234", "BOOLEAN"))

        self.assertTrue(Validator.checkType("1234.0", "STRING"))
        self.assertFalse(Validator.checkType("1234.0", "INT"))
        self.assertTrue(Validator.checkType("1234.00", "DECIMAL"))
        self.assertFalse(Validator.checkType("1234.0", "LONG"))
        self.assertFalse(Validator.checkType("1234.0", "BOOLEAN"))

    def test_schema_optional_field(self):
        """Test optional fields."""
        schema = self.schema
        interfaces = self.interfaces
        record = {
            "test1": "hello",
            "test2": "1.0",
            "test3": "YES",
            "test4": "1",
            "test5": "1",
         }
        self.assertTrue(Validator.validate(
            record, schema, "award", interfaces)[0])
        record["test5"] = ""
        self.assertTrue(Validator.validate(
            record, schema, "award", interfaces)[0])
        record["test5"] = "s"
        self.assertFalse(Validator.validate(
            record, schema, "award", interfaces)[0])
        record["test5"] = ""
        record["test3"] = ""
        self.assertFalse(Validator.validate(
            record, schema, "award", interfaces)[0])

if __name__ == '__main__':
    unittest.main()