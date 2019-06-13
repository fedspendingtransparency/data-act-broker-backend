from dataactcore.config import CONFIG_BROKER
import os

SQL_DIR = os.path.join(CONFIG_BROKER["path"], "dataactcore", "scripts", "raw_sql")


def gather_file_sql(file_type, submission_id):
    """ Return raw queries representing the file requested

        Args:
            file_type: type of file to generate (specifically 'E', 'F')
            submission_id: submission to get data from

        Returns:
            raw string query representing File E data

        Raises:
            ValueError: invalid file_type
    """
    if file_type not in ['E', 'F']:
        raise ValueError('Invalid valid type: {}'.format(file_type))

    # Get the raw SQL to work with
    with open(os.path.join(SQL_DIR, 'file{}.sql'.format(file_type)), 'r') as sql_file:
        file_sql = sql_file.read()

    # Remove newlines (write_stream_query doesn't like them) and add the submission ID to the query
    file_sql = file_sql.replace('\n', ' ')
    file_sql = file_sql.format(submission_id)
    return file_sql


def generate_file_e_sql(submission_id):
    """ Return two raw queries representing the E File

        Args:
            submission_id: submission to get data from

        Returns:
            raw string query representing File E data
    """
    return gather_file_sql('E', submission_id)


def generate_file_f_sql(submission_id):
    """ Return raw query representing the F File

        Args:
            submission_id: submission to get data from

        Returns:
            raw string query representing File F data
    """
    return gather_file_sql('F', submission_id)
