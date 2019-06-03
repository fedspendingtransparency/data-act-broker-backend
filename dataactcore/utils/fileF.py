from dataactcore.config import CONFIG_BROKER
import os


def generate_f_file_query(submission_id):
    """ Return two raw queries representing the F File

        Args:
            submission_id: submission to get data from

        Returns:
            raw string query representing File F contract data
            raw string query representing File F grant data
    """
    file_f_dir = os.path.join(CONFIG_BROKER["path"], "dataactcore", "scripts", "raw_sql")

    with open(os.path.join(file_f_dir, 'fileF.sql'), 'r') as file_f_contracts:
        file_f_contracts_sql = file_f_contracts.read()
    file_f_contracts_sql = file_f_contracts_sql.replace('\n', ' ')
    return file_f_contracts_sql.format(submission_id)
