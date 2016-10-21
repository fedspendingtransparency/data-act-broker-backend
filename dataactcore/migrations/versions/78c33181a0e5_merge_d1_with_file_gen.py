"""merge-d1-with-file-gen

Revision ID: 78c33181a0e5
Revises: 73db7d2cc754, 7d4f322c7661
Create Date: 2016-09-06 13:36:09.249432

"""

# revision identifiers, used by Alembic.
revision = '78c33181a0e5'
down_revision = ('73db7d2cc754', '7d4f322c7661')
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

