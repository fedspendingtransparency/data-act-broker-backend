import itertools
from operator import attrgetter

from dataactcore.models.lookups import FILE_TYPE, FILE_TYPE_DICT_NAME_LETTER


def report_file_name(submission_id, warning, file_type, cross_type=None):
    """Format the csv file name for the requested file.
    @todo: unify these file names"""
    if cross_type:
        report_type_str = 'warning_' if warning else ''
        file_letters = 'File_{}_to_{}_'.format(FILE_TYPE_DICT_NAME_LETTER[file_type],
                                               FILE_TYPE_DICT_NAME_LETTER[cross_type])
        return "submission_{}_crossfile_{}{}{}_{}.csv".format(submission_id, report_type_str, file_letters, file_type,
                                                              cross_type)
    else:
        report_type_str = 'warning_' if warning else 'error_'
        file_letter = 'File_{}_'.format(FILE_TYPE_DICT_NAME_LETTER[file_type])
        return "submission_{}_{}{}_{}report.csv".format(submission_id, file_letter, file_type, report_type_str)


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
