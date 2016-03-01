import unittest
from dataactcore.models.validationModels import  FieldType,RuleType, FileColumn, Rule
from dataactvalidator.validation_handlers.validator import Validator
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder

class ValidatorTests(unittest.TestCase) :

    def __init__(self,methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(ValidatorTests,self).__init__(methodName=methodName)
        self.interfaces = InterfaceHolder()

    def test_types(self) :
        assert(Validator.checkType("1234Test","STRING")), "Invalid Type"
        assert(not Validator.checkType("1234Test","INT")), "Invalid Type"
        assert( not Validator.checkType("1234Test","DECIMAL")), "Invalid Type"
        assert(not Validator.checkType("1234Test","BOOLEAN")), "Invalid Type"

        assert(not Validator.checkType("","STRING")), "Valid Type"
        assert(Validator.checkType("","INT")), "Valid Type"
        assert(Validator.checkType("","DECIMAL")), "Valid Type"
        assert(not Validator.checkType("","BOOLEAN")), "Valid Type"

        assert( Validator.checkType("01234","STRING")), "Valid Type"
        assert( Validator.checkType("1234","INT")), "Valid Type"
        assert( Validator.checkType("1234","DECIMAL")), "Valid Type"
        assert( not Validator.checkType("1234","BOOLEAN")), "Invalid Type"

        assert( Validator.checkType("1234.0","STRING")), "Valid Type"
        assert( not Validator.checkType("1234.0","INT")), "Invalid Type"
        assert( Validator.checkType("1234.00","DECIMAL")), "valid Type"
        assert( not Validator.checkType("1234.0","BOOLEAN")), "Invalid Type"

    def test_type_coversion(self):
        assert( isinstance(Validator.getType("1234.0","STRING"),basestring)), "Invalid Type"
        assert( type(Validator.getType("10","INT")) == int), "Invalid Type"
        assert( isinstance(Validator.getType("YES","BOOLEAN"),basestring)), "Invalid Type"
        assert(type( Validator.getType("1234.2","DECIMAL"))==float), "Invalid Type"



    def createSchema(self) :
        stringType =  FieldType()
        stringType.field_type_id = 1
        stringType.name= "STRING"

        intType =  FieldType()
        intType.field_type_id = 2
        intType.name= "INT"

        floatType =  FieldType()
        floatType.field_type_id = 3
        floatType.name= "DECIMAL"

        booleanType =  FieldType()
        booleanType.field_type_id = 4
        booleanType.name= "BOOLEAN"

        column1 =  FileColumn()
        column1.file_column_id = 1
        column1.name= "test1"
        column1.required = True
        column1.field_type = stringType
        column1.file_id = 1
        column1.populateFile()

        column2 =  FileColumn()
        column2.file_column_id = 2
        column2.name= "test2"
        column2.required = True
        column2.field_type = floatType
        column2.file_id = 1
        column2.populateFile()

        column3 =  FileColumn()
        column3.file_column_id = 3
        column3.name= "test3"
        column3.required = True
        column3.field_type = booleanType
        column3.file_id = 1
        column3.populateFile()

        column4 =  FileColumn()
        column4.file_column_id = 3
        column4.name= "test4"
        column4.required = True
        column4.field_type =intType
        column4.file_id = 1
        column4.populateFile()

        column5 =  FileColumn()
        column5.file_column_id = 3
        column5.name= "test5"
        column5.required = False
        column5.field_type =intType
        column5.file_id = 1
        column5.populateFile()

        column6 =  FileColumn()
        column6.file_column_id = 6
        column6.name= "test6"
        column6.required = False
        column6.field_type =stringType
        column6.file_id = 1
        column6.populateFile()

        schema =  {
            "test1" :column1,
            "test2" :column2,
            "test3" :column3,
            "test4" :column4,
            "test5" :column5,
            "test6" :column6
        }
        return schema

    def test_schema_optional_field(self):

        schema = self.createSchema()
        record = {
            "test1" : "hello" ,
            "test2" : "1.0",
            "test3" :"YES",
            "test4" :"1",
            "test5" :"1",
         }
        assert( Validator.validate(record,[],schema, None,self.interfaces)[0]),"Fields are not correct type"
        record["test5"] = ""


        assert( Validator.validate(record,[],schema, None,self.interfaces)[0]),"Blank optional field is valid"

        record["test5"] = "s"
        assert(not Validator.validate(record,[],schema, None,self.interfaces)[0]),"Incorrect Field Type for optional field"

        record["test5"] = ""
        record["test3"] = ""
        assert(not Validator.validate(record,[],schema, None,self.interfaces)[0]),"Incorrect Field Type for field"

    def test_schema_rules(self):
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

        schema = self.createSchema()
        rule1 =  Rule()
        rule1.rule_type = equalRule
        rule1.file_column = schema["test1"]
        rule1.rule_text_1  = "hello"

        rule2 =  Rule()
        rule2.rule_type = notRule
        rule2.file_column = schema["test1"]
        rule2.rule_text_1  = "bye"

        rule3 =  Rule()
        rule3.rule_type = lengthRule
        rule3.file_column = schema["test1"]
        rule3.rule_text_1  = "6"


        rule4 =  Rule()
        rule4.rule_type = equalRule
        rule4.file_column = schema["test3"]
        rule4.rule_text_1  = "YES"


        rule5 =  Rule()
        rule5.rule_type = equalRule
        rule5.file_column = schema["test4"]
        rule5.rule_text_1  = "44"

        rule6 =  Rule()
        rule6.rule_type = lessRule
        rule6.file_column = schema["test4"]
        rule6.rule_text_1  = "45"

        rule7 =  Rule()
        rule7.rule_type = greaterRule
        rule7.file_column = schema["test2"]
        rule7.rule_text_1  = ".5"

        rule8 = Rule()
        rule8.rule_type = setRule
        rule8.file_column = schema["test6"]
        rule8.rule_text_1 = "X, F, A"


        rules = [rule1,rule2,rule3,rule4,rule5,rule6,rule7,rule8]
        record = {
            "test1" : "hello" ,
            "test2" : "1.0",
            "test3" :"YES",
            "test4" :"44",
            "test5" :"1",
            "test6" :"X"
        }
        assert(Validator.validate(record,rules,schema,"award",self.interfaces)[0]),"Values do not match rules"

        record = {
            "test1" : "goodbye" ,
            "test2" : ".4",
            "test3" :"NO",
            "test4" :"45",
            "test5" :"1",
            "test6" :"Q"
        }
        assert( not Validator.validate(record,[rule3],schema,"award",self.interfaces)[0]),"Rule for test1 passed"
        assert( not Validator.validate(record,[rule4],schema,"award",self.interfaces)[0]),"Rule for test3 passed"
        assert( not Validator.validate(record,[rule5],schema,"award",self.interfaces)[0]),"Rule for test4 passed"
        assert( not Validator.validate(record,[rule6],schema,"award",self.interfaces)[0]),"Rule for test4 passed"
        assert( not Validator.validate(record,[rule7],schema,"award",self.interfaces)[0]),"Rule for test2 passed"
        assert( not Validator.validate(record,[rule8],schema,"award",self.interfaces)[0]),"Rule for test6 passed"
        assert( not Validator.validate(record,rules,schema,"award",self.interfaces)[0]),"Rules passed"
if __name__ == '__main__':
    unittest.main()
