from dataactcore.emr.models.broker_submissions import *
from dataactcore.emr.models.broker_external import *
from dataactcore.emr.models.reference import *
from dataactcore.emr.models.usas import *

DELTA_MODEL_CLASSES = [
    DEFCDelta,
]
DELTA_MODELS = {model.table_name: model for model in DELTA_MODEL_CLASSES}
