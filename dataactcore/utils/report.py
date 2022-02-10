from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import filename_fyp_sub_format
from dataactcore.models.jobModels import Submission, Job, FormatChangeDate
from dataactcore.models.lookups import FILE_TYPE_DICT_NAME_LETTER, JOB_TYPE_DICT, FILE_TYPE_DICT, REPORT_FILENAMES


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

    sub = sess.query(Submission).filter_by(submission_id=submission_id).one()
    fillin_vals = {
        'submission_id': submission_id,
        'file_type': file_type,
        'file_letter': FILE_TYPE_DICT_NAME_LETTER[file_type],
        'cross_type': cross_type,
        'cross_letter': FILE_TYPE_DICT_NAME_LETTER[cross_type] if cross_type else '',
        'report_type': 'warning' if warning else 'error',
        'FYP': '_{}'.format(filename_fyp_sub_format(sub)) if not sub.d2_submission else ''
    }

    if cross_type:
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['validation']).one()
    else:
        job = sess.query(Job).filter_by(submission_id=submission_id, job_type_id=JOB_TYPE_DICT['csv_record_validation'],
                                        file_type_id=FILE_TYPE_DICT[file_type]).one()
    daims_change = sess.query(FormatChangeDate.change_date).filter_by(name='DAIMS 2.0').one_or_none()
    dev_8325_change = sess.query(FormatChangeDate.change_date).filter_by(name='DEV-8325').one_or_none()

    if daims_change and job.updated_at < daims_change.change_date:
        ew_version = 'PRE-DAIMS 2.0'
        if cross_type:
            fillin_vals['report_type'] = 'warning_' if warning else ''
    elif dev_8325_change and job.updated_at < dev_8325_change.change_date:
        ew_version = 'DAIMS 2.0'
        if cross_type:
            fillin_vals['report_type'] = 'warning_' if warning else ''
        else:
            fillin_vals['report_type'] = 'warning_' if warning else 'error_'
    else:
        ew_version = 'DEV-8325'
    ew_type = 'cross-file' if cross_type else 'file'

    return REPORT_FILENAMES[ew_version][ew_type].format(**fillin_vals)
