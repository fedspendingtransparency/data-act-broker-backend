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

def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()

def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()

def upgrade_data_broker():
    # Data is far enough forward that either there will be no data or
    # the data has long since been updated. We can just pass this validation
    pass

def downgrade_data_broker():
    # No backwards data migration here. It's better for
    # all submissions to have the fiscal_period info
    # set consistently
    pass

