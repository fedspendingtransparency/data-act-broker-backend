import pandas as pd

from pandas import isnull

from dataactcore.models.domainModels import concat_display_tas_dict
from dataactcore.models.lookups import FIELD_TYPE_DICT_ID

from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactvalidator.validation_handlers.validator import Validator
from dataactvalidator.validation_handlers.validationError import ValidationError


def clean_col(value):
    if isnull(value) or not str(value).strip():
        return None

    # Trim and remove extra quotes around the outside. If removing quotes and stripping leaves nothing, set
    # it to None
    value = str(value).strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1].strip()
        if not value:
            return None

    return value


# TODO: Check if we even need this
def clean_numbers(value):
    if value is not None:
        temp_value = value.replace(',', '')
        if FieldCleaner.is_numeric(temp_value):
            return temp_value
    return value


def concat_flex(row):
    return ', '.join([name + ': ' + (row[name] or '') for name in sorted(row.keys()) if name is not 'row_number'])


def derive_unique_id(row, is_fabs):
    if not is_fabs:
        return 'TAS: {}'.format(concat_display_tas_dict(row))

    return 'AssistanceTransactionUniqueKey: {}'.format(row['afa_generated_unique'])


def derive_fabs_awarding_sub_tier(row, office_list):
    if not row['awarding_sub_tier_agency_c']:
        return office_list.get(row['awarding_office_code'])
    return row['awarding_sub_tier_agency_c']


def derive_fabs_afa_generated_unique(row):
    return (row['awarding_sub_tier_agency_c'] or '-none-') + '_' + \
           (row['fain'] or '-none-') + '_' + \
           (row['uri'] or '-none-') + '_' + \
           (row['cfda_number'] or '-none-') + '_' + \
           (row['award_modification_amendme'] or '-none-')


def derive_fabs_unique_award_key(row):
    if str(row['record_type']) == '1':
        unique_award_key_list = ['ASST_AGG', row['uri'] or '-none-']
    else:
        unique_award_key_list = ['ASST_NON', row['fain'] or '-none-']

    unique_award_key_list.append(row['awarding_sub_tier_agency_c'] or '-none-')

    return '_'.join(unique_award_key_list).upper()


def apply_label(row, labels, is_fabs):
    if is_fabs and labels and row['Field Name'] in labels:
        return labels[row['Field Name']]
    return ''


def gather_flex_fields(row, flex_data):
    if flex_data is not None:
        return flex_data.loc[flex_data['row_number'] == row['Row Number'], 'concatted'].values[0]
    return ''


def valid_type(row, csv_schema):
    current_field = csv_schema[row['Field Name']]
    return Validator.check_type(row['Value Provided'], FIELD_TYPE_DICT_ID[current_field.field_types_id])


def expected_type(row, csv_schema):
    current_field = csv_schema[row['Field Name']]
    return 'This field must be a {}'.format(FIELD_TYPE_DICT_ID[current_field.field_types_id].lower())


def valid_length(row, csv_schema):
    current_field = csv_schema[row['Field Name']]
    if current_field.length:
        return len(row['Value Provided']) <= current_field.length
    return True


def expected_length(row, csv_schema):
    current_field = csv_schema[row['Field Name']]
    return 'Max length: {}'.format(current_field.length)


def update_field_name(row, short_cols):
    if row['Field Name'] in short_cols:
        return short_cols[row['Field Name']]
    return row['Field Name']


def add_field_name_to_value(row):
    return row['Field Name'] + ': ' + row['Value Provided']


def check_required(data, required, required_labels, report_headers, short_cols, flex_data, is_fabs):
    req_data = data[required + ['row_number', 'unique_id']]
    errors = pd.melt(req_data, id_vars=['row_number', 'unique_id'], value_vars=required,
                     var_name='Field Name', value_name='Value Provided')
    errors = errors[errors['Value Provided'].isnull()]
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Value Provided'] = ''
    errors['Error Message'] = ValidationError.requiredErrorMsg
    errors['Expected Value'] = '(not blank)'
    errors['Difference'] = ''
    if not errors.empty:
        errors['Rule Label'] = errors.apply(lambda x: apply_label(x, required_labels, is_fabs), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
    else:
        errors['Rule Label'] = ''
        errors['Flex Field'] = ''
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.requiredError
    return errors


def check_type(data, type_fields, type_labels, report_headers, csv_schema, short_cols, flex_data, is_fabs):
    type_data = data[type_fields + ['row_number', 'unique_id']]
    errors = pd.melt(type_data, id_vars=['row_number', 'unique_id'], value_vars=type_fields,
                     var_name='Field Name', value_name='Value Provided')
    errors = errors[~errors['Value Provided'].isnull()]
    if not errors.empty:
        errors['matches_type'] = errors.apply(lambda x: valid_type(x, csv_schema), axis=1)
        errors = errors[~errors['matches_type']]
        errors.drop(['matches_type'], axis=1, inplace=True)
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Error Message'] = ValidationError.typeErrorMsg
    errors['Difference'] = ''
    if not errors.empty:
        errors['Expected Value'] = errors.apply(lambda x: expected_type(x, csv_schema), axis=1)
        errors['Rule Label'] = errors.apply(lambda x: apply_label(x, type_labels, is_fabs), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
        errors['Value Provided'] = errors.apply(lambda x: add_field_name_to_value(x), axis=1)
    else:
        errors['Expected Value'] = ''
        errors['Rule Label'] = ''
        errors['Flex Field'] = ''
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.typeError
    return errors


def check_length(data, length_fields, report_headers, csv_schema, short_cols, flex_data, type_error_rows):
    length_data = data[length_fields + ['row_number', 'unique_id']]
    errors = pd.melt(length_data, id_vars=['row_number', 'unique_id'], value_vars=length_fields,
                     var_name='Field Name', value_name='Value Provided')
    errors = errors[~errors['Value Provided'].isnull() & ~errors['row_number'].isin(type_error_rows)]
    if not errors.empty:
        errors['valid_length'] = errors.apply(lambda x: valid_length(x, csv_schema), axis=1)
        errors = errors[~errors['valid_length']]
        errors.drop(['valid_length'], axis=1, inplace=True)
    errors.rename(columns={'row_number': 'Row Number', 'unique_id': 'Unique ID'}, inplace=True)
    errors = errors.reset_index()
    errors['Error Message'] = ValidationError.lengthErrorMsg
    errors['Difference'] = ''
    errors['Rule Label'] = ''
    if not errors.empty:
        errors['Expected Value'] = errors.apply(lambda x: expected_length(x, csv_schema), axis=1)
        errors['Flex Field'] = errors.apply(lambda x: gather_flex_fields(x, flex_data), axis=1)
        errors['Field Name'] = errors.apply(lambda x: update_field_name(x, short_cols), axis=1)
        errors['Value Provided'] = errors.apply(lambda x: add_field_name_to_value(x), axis=1)
    else:
        errors['Expected Value'] = ''
        errors['Flex Field'] = ''
    errors = errors[report_headers]
    errors['error_type'] = ValidationError.lengthError
    return errors
