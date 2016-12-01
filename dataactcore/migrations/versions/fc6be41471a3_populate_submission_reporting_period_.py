"""populate-submission-reporting-period-info

Revision ID: fc6be41471a3
Revises: d7476f20b59f
Create Date: 2016-11-27 23:46:52.922968

"""

# revision identifiers, used by Alembic.
revision = 'fc6be41471a3'
down_revision = 'd7476f20b59f'
branch_labels = None
depends_on = None

from alembic import op
from sqlalchemy.orm.session import Session

from dataactcore.models.jobModels import Submission

# Following two functions are the same ones used in
# the Job models to automatically set fiscal period/year
# information when a new submission is created.
# There's likely a clever way to re-use that code,
# which uses a context to get all of the required
# data. However, since this is a small, one-time
# migration, it just uses a copied version of
# that code.
def generate_fiscal_year(reporting_end_date):
    """ Generate fiscal year based on the date provided """
    reporting_end_date = reporting_end_date
    year = reporting_end_date.year
    if reporting_end_date.month in [10,11,12]:
        year += 1
    return year

def generate_fiscal_period(reporting_end_date):
    """ Generate fiscal period based on the date provided """
    reporting_end_date = reporting_end_date
    period = (reporting_end_date.month + 3) % 12
    period = 12 if period == 0 else period
    return period

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()

def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

def upgrade_data_broker():
    # For older submissions, populate reporting_fiscal_year and
    # reporting_fiscal_period. When we first added these columns,
    # it wasn't critical that existing submissions had accurate
    # information, so we didn't do a data migration. However, the
    # check_status_route now needs this information to correctly
    # format submission start/end dates, so we need to make sure
    # all submissions have it.
    sess = Session(bind=op.get_bind())
    submissions = sess.query(Submission).all()
    for s in submissions:
        if s.reporting_fiscal_year == 0:
            s.reporting_fiscal_year = generate_fiscal_year(s.reporting_end_date)
        if s.reporting_fiscal_period == 0:
            s.reporting_fiscal_period = generate_fiscal_period(s.reporting_end_date)
    sess.commit()

def downgrade_data_broker():
    # No backwards data migration here. It's better for
    # all submissions to have the fiscal_period info
    # set consistently
    pass

