from dataactcore.config import CONFIG_DB
from dataactcore.broker_logging import configure_logging
from dataactcore.scripts.setup.database_setup import create_database


def setup_job_queue_db():
    """Create job tracker tables from model metadata."""
    create_database(CONFIG_DB["job_queue_db_name"])


if __name__ == "__main__":
    configure_logging()
    setup_job_queue_db()
