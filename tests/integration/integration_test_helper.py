import calendar
from datetime import datetime

from dataactcore.models.jobModels import Submission


def insert_submission(sess, submission_user_id, cgac_code=None, start_date=None, end_date=None,
                      is_quarter=False, number_of_errors=0, publish_status_id=1, is_fabs=False):
    """Insert one submission into job tracker and get submission ID back."""
    publishable = True if number_of_errors == 0 else False
    end_date = datetime.strptime(end_date, '%m/%Y')
    end_date = datetime.strptime(
        str(end_date.year) + '/' +
        str(end_date.month) + '/' +
        str(calendar.monthrange(end_date.year, end_date.month)[1]),
        '%Y/%m/%d'
    ).date()
    sub = Submission(created_at=datetime.utcnow(),
                     user_id=submission_user_id,
                     cgac_code=cgac_code,
                     reporting_start_date=datetime.strptime(start_date, '%m/%Y'),
                     reporting_end_date=end_date,
                     is_quarter_format=is_quarter,
                     number_of_errors=number_of_errors,
                     publish_status_id=publish_status_id,
                     publishable=publishable,
                     d2_submission=is_fabs)
    sess.add(sub)
    sess.commit()
    return sub.submission_id
