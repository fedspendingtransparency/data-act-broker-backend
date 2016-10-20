"""Merging-sf-133-year-and-file-gen

Revision ID: 1ae491ca0925
Revises: c9090bf7f0a8, 78c33181a0e5
Create Date: 2016-09-07 13:49:23.335732

"""

# revision identifiers, used by Alembic.
revision = '1ae491ca0925'
down_revision = ('c9090bf7f0a8', '78c33181a0e5')
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

