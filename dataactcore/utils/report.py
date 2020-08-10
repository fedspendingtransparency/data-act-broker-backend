import itertools
from operator import attrgetter
from datetime import datetime

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.jobModels import Job
from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT_NAME_LETTER, JOB_TYPE_DICT, FILE_TYPE_DICT

DAIMS_THRESHOLD = datetime.strptime('07-13-2020 21:53', '%m-%d-%Y %H:%M')


def report_file_name(submission_id, warning, file_type, cross_type=None):
    """ Format the csv file name for the requested file

        Args:
            submission_id: the submission's id
            warning: whether it's a warning or error (True if warning)
            file_type: the file type name associated with the report
            cross_type: cross file type name associated with the report

        Returns:
            string of the report file name
    """
    sess = GlobalDB.db().session

    if cross_type:
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['validation']).one()
    else:
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                                        file_type_id=FILE_TYPE_DICT[file_type]).one()

    if job.updated_at < DAIMS_THRESHOLD:
        if cross_type:
            report_type_str = 'warning_' if warning else ''
            return "submission_{}_cross_{}{}_{}.csv".format(submission_id, report_type_str, file_type, cross_type)
        else:
            report_type_str = 'warning' if warning else 'error'
            return "submission_{}_{}_{}_report.csv".format(submission_id, file_type, report_type_str)
    else:
        if cross_type:
            report_type_str = 'warning_' if warning else ''
            return "submission_{}_crossfile_{}File_{}_to_{}_{}_{}.csv".format(submission_id, report_type_str,
                                                                              FILE_TYPE_DICT_NAME_LETTER[file_type],
                                                                              FILE_TYPE_DICT_NAME_LETTER[cross_type],
                                                                              file_type,
                                                                              cross_type)
        else:
            report_type_str = 'warning_' if warning else 'error_'
            return "submission_{}_File_{}_{}_{}report.csv".format(submission_id,
                                                                  FILE_TYPE_DICT_NAME_LETTER[file_type],
                                                                  file_type, report_type_str)


def get_cross_file_pairs():
    """
    Create a list that represents each possible combination of files used
    in cross file validations.

    Returns:
        a list of tuples, where the first tuple represents file #1 in a pair
        and the second tuple represent file #2 in a pair
    """
    # make sure the list is sorted by files' order attributes to ensure that files
    # in pairs are always listed in the same order
    crossfile_sorted = sorted([f for f in FILE_TYPE if f.crossfile], key=attrgetter('order'))
    # create unique combinations of all files eligible for cross-file validation
    crossfile_combos = itertools.combinations(crossfile_sorted, 2)
    return list(map(list, crossfile_combos))
