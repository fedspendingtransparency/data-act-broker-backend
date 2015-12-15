import unittest
from  handlers.validator import Validator
from dataactcore.models.validationModels import  FileType, FieldType,RuleType, FileColumn, Rule
class ValidatorTests(unittest.TestCase) :

    def __init__(self,methodName):
        """ Run scripts to clear the job tables and populate with a defined test set """
        super(ValidatorTests,self).__init__(methodName=methodName)

    def test_types(self) :
        assert(Validator.checkType("1234Test","STRING")), "Invalid Type"
        assert(not Validator.checkType("1234Test","INT")), "Invalid Type"
        assert( not Validator.checkType("1234Test","DECIMAL")), "Invalid Type"
        assert(not Validator.checkType("1234Test","BOOLEAN")), "Invalid Type"

        assert(not Validator.checkType("","STRING")), "Valid Type"
        assert(not Validator.checkType("","INT")), "Valid Type"
        assert( not Validator.checkType("","DECIMAL")), "Valid Type"
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

        column2 =  FileColumn()
        column2.file_column_id = 2
        column2.name= "test2"
        column2.required = True
        column2.field_type = floatType

        column3 =  FileColumn()
        column3.file_column_id = 3
        column3.name= "test3"
        column3.required = True
        column3.field_type = booleanType

        column4 =  FileColumn()
        column4.file_column_id = 3
        column4.name= "test4"
        column4.required = True
        column4.field_type =intType

        column5 =  FileColumn()
        column5.file_column_id = 3
        column5.name= "test5"
        column5.required = False
        column5.field_type =intType

        schema =  {
            "test1" :column1,
            "test2" :column2,
            "test3" :column3,
            "test4" :column4,
            "test5" :column5
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
        assert( Validator.validate(record,[],schema)),"Fields are not correct type"
        record["test5"] = ""


        assert( Validator.validate(record,[],schema)),"Blank optional field is vaild"

        record["test5"] = "s"
        assert(not Validator.validate(record,[],schema)),"Incorrect Field Type for optional field"

        record["test5"] = ""
        record["test3"] = ""
        assert(not Validator.validate(record,[],schema)),"Incorrect Field Type for field"

    def test_schema_rules(self):
        pass 
if __name__ == '__main__':
    unittest.main()
