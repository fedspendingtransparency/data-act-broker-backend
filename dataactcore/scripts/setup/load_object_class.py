import os
import logging
import json
import requests
import pandas as pd
import datetime

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import ObjectClass
from dataactvalidator.health_check import create_app
from dataactcore.utils.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_object_class(base_path):
    """This function loads Object classes into the database

    Args:
        base_path: directory that contains the domain values files.
    """
    now = datetime.datetime.now()
    metrics_json = {
        "script_name": "load_object_class.py",
        "start_time": str(now),
        "records_received": 0,
        "duplicates_dropped": 0,
        "records_deleted": 0,
        "records_inserted": 0,
    }

    filename = os.path.join(base_path, "object_class.csv")
    try:
        # Update file from public S3 bucket
        object_class_url = "{}/object_class.csv".format(CONFIG_BROKER["usas_public_reference_url"])
        r = requests.get(object_class_url, allow_redirects=True)
        open(filename, "wb").write(r.content)
    except Exception:
        pass

    # Load object class lookup table
    logger.info("Loading Object Class File: object_class.csv")
    with create_app().app_context():
        sess = GlobalDB.db().session
        metrics_json["records_deleted"] = sess.query(ObjectClass).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            ObjectClass,
            {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
            {"object_class_code": {"pad_to_length": 3}},
        )
        metrics_json["records_received"] = len(data.index)
        # de-dupe
        data.drop_duplicates(subset=["object_class_code"], inplace=True)
        metrics_json["duplicates_dropped"] = metrics_json["records_received"] - len(data.index)
        # insert to db
        table_name = ObjectClass.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info("{} records inserted to {}".format(num, table_name))
    metrics_json["records_inserted"] = num

    update_external_data_load_date(now, datetime.datetime.now(), "object_class")

    metrics_json["duration"] = str(datetime.datetime.now() - now)

    with open("load_object_class_metrics.json", "w+") as metrics_file:
        json.dump(metrics_json, metrics_file)


if __name__ == "__main__":
    configure_logging()
