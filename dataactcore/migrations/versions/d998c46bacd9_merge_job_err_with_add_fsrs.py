"""merge job_err with add_fsrs

Revision ID: d998c46bacd9
Revises: 361fbffcf08b, caa6895e7450
Create Date: 2016-08-26 19:09:39.554574

"""

# revision identifiers, used by Alembic.
revision = 'd998c46bacd9'
down_revision = ('361fbffcf08b', 'caa6895e7450')
branch_labels = None
depends_on = None

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    pass


def downgrade_data_broker():
    pass

