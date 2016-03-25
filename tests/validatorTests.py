import unittest
from dataactvalidator.models.validationModels import FieldType, RuleType, FileColumn, Rule
from dataactvalidator.validation_handlers.validator import Validator
from baseTest import BaseTest


class ValidatorTests(BaseTest):

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

    def test_type_conversion(self):
        """Test data type conversions."""
        self.assertIsInstance(Validator.getType("1234.0", "STRING"), basestring)
        self.assertIsInstance(Validator.getType("10", "INT"), int)
        self.assertIsInstance(Validator.getType("YES", "BOOLEAN"), basestring)
        self.assertIsInstance(Validator.getType("1234.2", "DECIMAL"), float)
        self.assertIsInstance(Validator.getType("400000000001", "LONG"), long)

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
            record, [], schema, "award", interfaces)[0])
        record["test5"] = ""
        self.assertTrue(Validator.validate(
            record, [], schema, "award", interfaces)[0])
        record["test5"] = "s"
        self.assertFalse(Validator.validate(
            record, [], schema, "award", interfaces)[0])
        record["test5"] = ""
        record["test3"] = ""
        self.assertFalse(Validator.validate(
            record, [], schema, "award", interfaces)[0])

    def test_schema_rules(self):
        """Test schema rules."""
        lessRule = RuleType()
        lessRule.name = "LESS"
        greaterRule = RuleType()
        greaterRule.name = "GREATER"
        lengthRule = RuleType()
        lengthRule.name = "LENGTH"
        equalRule = RuleType()
        equalRule.name = "EQUAL"
        notRule = RuleType()
        notRule.name = "NOT EQUAL"
        setRule = RuleType()
        setRule.name = "IN_SET"

        schema = self.schema
        interfaces = self.interfaces
        rule1 = Rule()
        rule1.rule_type = equalRule
        rule1.file_column = schema["test1"]
        rule1.rule_text_1 = "hello"
        rule1.rule_timing_id = 1

        rule2 = Rule()
        rule2.rule_type = notRule
        rule2.file_column = schema["test1"]
        rule2.rule_text_1 = "bye"
        rule2.rule_timing_id = 1

        rule3 = Rule()
        rule3.rule_type = lengthRule
        rule3.file_column = schema["test1"]
        rule3.rule_text_1 = "6"
        rule3.rule_timing_id = 1

        rule4 = Rule()
        rule4.rule_type = equalRule
        rule4.file_column = schema["test3"]
        rule4.rule_text_1 = "YES"
        rule4.rule_timing_id = 1

        rule5 = Rule()
        rule5.rule_type = equalRule
        rule5.file_column = schema["test4"]
        rule5.rule_text_1 = "44"
        rule5.rule_timing_id = 1

        rule6 = Rule()
        rule6.rule_type = lessRule
        rule6.file_column = schema["test4"]
        rule6.rule_text_1 = "45"
        rule6.rule_timing_id = 1

        rule7 = Rule()
        rule7.rule_type = greaterRule
        rule7.file_column = schema["test2"]
        rule7.rule_text_1 = ".5"
        rule7.rule_timing_id = 1

        rule8 = Rule()
        rule8.rule_type = setRule
        rule8.file_column = schema["test6"]
        rule8.rule_text_1 = "X, F, A"
        rule8.rule_timing_id = 1

        rules = [rule1, rule2, rule3, rule4, rule5, rule6, rule7, rule8]
        record = {
            "test1": "hello",
            "test2": "1.0",
            "test3": "YES",
            "test4": "44",
            "test5": "1",
            "test6": "X"
        }
        self.assertTrue(Validator.validate(
            record, rules, schema, "award", self.interfaces)[0])

        record = {
            "test1": "goodbye",
            "test2": ".4",
            "test3": "NO",
            "test4": "45",
            "test5": "1",
            "test6": "Q"
        }
        self.assertFalse(Validator.validate(
            record, [rule3], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, [rule4], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, [rule5], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, [rule6], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, [rule7], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, [rule8], schema, "award", interfaces)[0])
        self.assertFalse(Validator.validate(
            record, rules, schema, "award", interfaces)[0])

if __name__ == '__main__':
    unittest.main()
