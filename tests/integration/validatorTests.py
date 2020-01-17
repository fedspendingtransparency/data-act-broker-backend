from dataactcore.models.validationModels import FieldType, FileColumn
from dataactbroker.helpers.validation_helper import is_valid_type
from tests.integration.baseTestValidator import BaseTestValidator
from dataactcore.models.lookups import FIELD_TYPE_DICT


class ValidatorTests(BaseTestValidator):

    @classmethod
    def setUpClass(cls):
        """ Set up class-wide resources (test data) """
        super(ValidatorTests, cls).setUpClass()
        # TODO: refactor into a pytest fixture

        # create test schema
        string_type = FieldType()
        string_type.field_type_id = 1
        string_type.name = 'STRING'

        int_type = FieldType()
        int_type.field_type_id = 2
        int_type.name = 'INT'

        float_type = FieldType()
        float_type.field_type_id = 3
        float_type.name = 'DECIMAL'

        boolean_type = FieldType()
        boolean_type.field_type_id = 4
        boolean_type.name = 'BOOLEAN'

        long_type = FieldType()
        long_type.field_type_id = 5
        long_type.name = 'LONG'

        column1 = FileColumn()
        column1.file_column_id = 1
        column1.name = 'test1'
        column1.required = True
        column1.field_type = string_type
        column1.field_types_id = FIELD_TYPE_DICT[string_type.name]
        column1.file_id = 1

        column2 = FileColumn()
        column2.file_column_id = 2
        column2.name = 'test2'
        column2.required = True
        column2.field_type = float_type
        column2.field_types_id = FIELD_TYPE_DICT[float_type.name]
        column2.file_id = 1

        column3 = FileColumn()
        column3.file_column_id = 3
        column3.name = 'test3'
        column3.required = True
        column3.field_type = boolean_type
        column3.field_types_id = FIELD_TYPE_DICT[boolean_type.name]
        column3.file_id = 1

        column4 = FileColumn()
        column4.file_column_id = 3
        column4.name = 'test4'
        column4.required = True
        column4.field_type = int_type
        column4.field_types_id = FIELD_TYPE_DICT[int_type.name]
        column4.file_id = 1

        column5 = FileColumn()
        column5.file_column_id = 3
        column5.name = 'test5'
        column5.required = False
        column5.field_type = int_type
        column5.field_types_id = FIELD_TYPE_DICT[int_type.name]
        column5.file_id = 1

        column6 = FileColumn()
        column6.file_column_id = 6
        column6.name = 'test6'
        column6.required = False
        column6.field_type = string_type
        column6.field_types_id = FIELD_TYPE_DICT[string_type.name]
        column6.file_id = 1

        column7 = FileColumn()
        column7.file_column_id = 7
        column7.name = 'test7'
        column7.required = False
        column7.field_type = long_type
        column7.field_types_id = FIELD_TYPE_DICT[long_type.name]
        column7.file_id = 1

        cls.schema = {
            'test1': column1,
            'test2': column2,
            'test3': column3,
            'test4': column4,
            'test5': column5,
            'test6': column6,
            'test7': column7
        }

    def test_types(self):
        """Test data type checks."""
        self.assertTrue(is_valid_type('1234Test', 'STRING'))
        self.assertFalse(is_valid_type('1234Test', 'INT'))
        self.assertFalse(is_valid_type('1234Test', 'DECIMAL'))
        self.assertFalse(is_valid_type('1234Test', 'BOOLEAN'))
        self.assertFalse(is_valid_type('1234Test', 'LONG'))

        self.assertTrue(is_valid_type('', 'STRING'))
        self.assertTrue(is_valid_type('', 'INT'))
        self.assertTrue(is_valid_type('', 'DECIMAL'))
        self.assertTrue(is_valid_type('', 'BOOLEAN'))
        self.assertTrue(is_valid_type('', 'LONG'))

        self.assertTrue(is_valid_type('01234', 'STRING'))
        self.assertTrue(is_valid_type('01234', 'INT'))
        self.assertTrue(is_valid_type('01234', 'DECIMAL'))
        self.assertTrue(is_valid_type('01234', 'LONG'))
        self.assertFalse(is_valid_type('01234', 'BOOLEAN'))

        self.assertTrue(is_valid_type('1234.0', 'STRING'))
        self.assertFalse(is_valid_type('1234.0', 'INT'))
        self.assertTrue(is_valid_type('1234.00', 'DECIMAL'))
        self.assertFalse(is_valid_type('1234.0', 'LONG'))
        self.assertFalse(is_valid_type('1234.0', 'BOOLEAN'))
