from dataactcore.config import CONFIG_BROKER
import os


def generate_f_file_queries(submission_id):
    """ Return two raw queries representing the F File

        Args:
            submission_id: submission to get data from

        Returns:
            raw string query representing File F contract data
            raw string query representing File F grant data
    """
    file_f_dir = os.path.join(CONFIG_BROKER["path"], "dataactcore", "scripts", "raw_sql")

    with open(os.path.join(file_f_dir, 'fileF_contracts.sql'), 'r') as file_f_contracts:
        file_f_contracts_sql = file_f_contracts.read()
    with open(os.path.join(file_f_dir, 'fileF_grants.sql'), 'r') as file_f_grants:
        file_f_grants_sql = file_f_grants.read()
    file_f_contracts_sql = file_f_contracts_sql.replace('\n', ' ')
    file_f_grants_sql = file_f_grants_sql.replace('\n', ' ')
    return file_f_contracts_sql.format(submission_id), file_f_grants_sql.format(submission_id)
