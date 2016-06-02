from dataactcore.models.validationInterface import ValidationInterface
from dataactcore.utils.statusCode import StatusCode
from dataactcore.utils.jsonResponse import JsonResponse

class ValidationBrokerInterface(ValidationInterface):
    """ Responsible for all interaction with the validation database

    Instance fields:
    engine -- sqlalchemy engine for generating connections and sessions
    connection -- sqlalchemy connection for executing direct SQL statements
    session -- sqlalchemy session for ORM usage
    """