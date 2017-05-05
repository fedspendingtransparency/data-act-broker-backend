"""Create submission_updated_at_view

Revision ID: f15485f0092b
Revises: 4ec514fa1f13
Create Date: 2017-05-05 09:50:57.693377

"""

# revision identifiers, used by Alembic.
revision = 'f15485f0092b'
down_revision = '4ec514fa1f13'
branch_labels = None
depends_on = None

from alembic import op


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    op.execute("""
            CREATE OR REPLACE VIEW submission_updated_at_view (submission_id, updated_at) AS
            SELECT temp.submission_id, max(temp.updated_at) as updated_at
            FROM (SELECT sub.submission_id, sub.updated_at
            FROM submission as sub
            UNION 
            SELECT job.submission_id, job.updated_at
            FROM job, submission as sub_2
            WHERE job.submission_id=sub_2.submission_id) as temp
            GROUP BY submission_id
        """)


def downgrade_data_broker():
    op.execute("DROP VIEW submission_updated_at_view")
