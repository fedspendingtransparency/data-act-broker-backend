import argparse

from sqlalchemy import MetaData, Table
from sqlalchemy.sql import update

from dataactcore.interfaces.db import GlobalDB
from dataactvalidator.health_check import create_app


def reset_alembic(alembic_version):

    with create_app().app_context():
        db = GlobalDB.db()

        engine = db.engine
        sess = db.session
        metadata = MetaData(bind=engine)
        alembic_table = Table('alembic_version', metadata, autoload=True)
        u = update(alembic_table)
        u = u.values({"version_num": alembic_version})
        sess.execute(u)
        sess.commit()

parser = argparse.ArgumentParser(description="Reset alembic version table.")
parser.add_argument(
    'version', help="Version to set the Alembic migration table to.")
v = vars(parser.parse_args())['version']
reset_alembic(v)
