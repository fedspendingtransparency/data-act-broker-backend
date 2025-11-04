from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from dataactcore.emr.models.delta_model import DeltaModel

class DEFCDelta(DeltaModel):
    s3_bucket = 'dti-broker-emr-qat'  # TODO: broker-external, broker-submissions, usas, analytics, etc.
    database = 'raw'
    table_name = 'defc'
    pk = 'defc_id'
    unique_constraints = [('code')]
    migration_history = [
        'add_test_column',
        'drop_test_column'
    ]

    @property
    def structure(self):
        return StructType([
            StructField('created_at', TimestampType(), True),
            StructField('updated_at', TimestampType(), True),
            StructField('defc_id', IntegerType(), False),
            StructField('code', StringType(), False),
            StructField('public_laws',  ArrayType(StringType(), True), True),
            StructField('public_law_short_titles', ArrayType(StringType(), True), True),
            StructField('group', StringType(), True),
            StructField('urls',  ArrayType(StringType(), True), True),
            StructField('is_valid', BooleanType(), False),
            StructField('earliest_pl_action_date', TimestampType(), True),
        ])