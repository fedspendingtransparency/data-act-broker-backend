from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import filename_fyp_sub_format
from dataactcore.models.jobModels import Submission, Job, FormatChangeDate
from dataactcore.models.lookups import FILE_TYPE_DICT_NAME_LETTER, JOB_TYPE_DICT, FILE_TYPE_DICT


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
        print(submission_id)
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['validation']).one()
    else:
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                                        file_type_id=FILE_TYPE_DICT[file_type]).one()

    daims_change = sess.query(FormatChangeDate.change_date).filter_by(name='DAIMS 2.0').one_or_none()
    dev_8325_change = sess.query(FormatChangeDate.change_date).filter_by(name='DEV-8325').one_or_none()
    if daims_change and job.updated_at < daims_change.change_date:
        if cross_type:
            report_type_str = 'warning_' if warning else ''
            return "submission_{}_cross_{}{}_{}.csv".format(submission_id, report_type_str, file_type, cross_type)
        else:
            report_type_str = 'warning' if warning else 'error'
            return "submission_{}_{}_{}_report.csv".format(submission_id, file_type, report_type_str)
    elif dev_8325_change and job.updated_at < dev_8325_change.change_date:
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
    else:
        fyp = filename_fyp_sub_format(sess.query(Submission).filter_by(submission_id=submission_id).one())
        report_type_str = 'warning' if warning else 'error'
        if cross_type:
            return "SubID-{}_File-{}-to-{}-crossfile-{}-report_{}.csv".format(submission_id,
                                                                              FILE_TYPE_DICT_NAME_LETTER[file_type],
                                                                              FILE_TYPE_DICT_NAME_LETTER[cross_type],
                                                                              report_type_str, fyp)
        else:
            return "SubID-{}_File-{}-{}-report_{}.csv".format(submission_id, FILE_TYPE_DICT_NAME_LETTER[file_type],
                                                              report_type_str, fyp)
