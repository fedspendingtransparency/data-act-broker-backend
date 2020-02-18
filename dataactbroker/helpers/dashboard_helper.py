from dataactbroker.helpers.filters_helper import file_filter
from dataactcore.models.lookups import FILE_TYPE_DICT_LETTER
from dataactcore.models.validationModels import RuleSetting

FILE_TYPES = ['A', 'B', 'C', 'cross-AB', 'cross-BC', 'cross-CD1', 'cross-CD2']


def generate_file_type(source_file_type_id, target_file_type_id):
    """ Helper function to generate the file type given the file types

        Args:
            source_file_type_id: id of the source file type
            target_file_type_id: id of the target file type (None for single-file)

        Return:
            string representing the file type
    """
    file_type = FILE_TYPE_DICT_LETTER.get(source_file_type_id)
    target_file_type = FILE_TYPE_DICT_LETTER.get(target_file_type_id)
    if file_type and target_file_type is None:
        return file_type
    elif file_type and target_file_type:
        return 'cross-{}'.format(''.join(sorted([file_type, target_file_type])))
    else:
        return None


def agency_has_settings(sess, agency_code, file):
    """ Helper function to determine if the agency has saved any settings of this file type

        Args:
            sess: the database connection
            agency_code: the agency code to work with
            file: the rule's file type

        Returns:
            True if the agency has saved their settings for this file type
    """

    # Check to see if agency has saved their settings for this file type
    query = sess.query(RuleSetting).filter(RuleSetting.agency_code == agency_code)
    query = file_filter(query, RuleSetting, [file])
    return query.count() > 0
