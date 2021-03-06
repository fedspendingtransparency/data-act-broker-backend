"""Create certified_comment table

Revision ID: e9c556a9f344
Revises: 0dc4a1fbb52e
Create Date: 2019-08-29 12:12:13.196702

"""

# revision identifiers, used by Alembic.
revision = 'e9c556a9f344'
down_revision = 'b998d20b46e6'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('certified_comment',
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('certified_comment_id', sa.Integer(), nullable=False),
        sa.Column('submission_id', sa.Integer(), nullable=False),
        sa.Column('file_type_id', sa.Integer(), nullable=False),
        sa.Column('comment', sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(['file_type_id'], ['file_type.file_type_id'], name='fk_file_type'),
        sa.ForeignKeyConstraint(['submission_id'], ['submission.submission_id'], name='fk_submission', ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('certified_comment_id'),
        sa.UniqueConstraint('submission_id', 'file_type_id', name='uniq_cert_comment_submission_file_type')
    )
    # ### end Alembic commands ###


def downgrade_data_broker():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('certified_comment')
    # ### end Alembic commands ###

