from dataactvalidator.filestreaming.tasLoader import TASLoader
from dataactvalidator.models.validationModels import TASLookup
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface


def loadTas(tasFile=None, dropIdx=True):
    """ Load all valid TAS combinations into database and index the TASLookup table """

    validatorDb = ValidatorValidationInterface()
    connection = validatorDb.engine.connect()

    # drop indexes
    table = TASLookup.__table__
    indexes = table.indexes
    if dropIdx:
        for i in indexes:
            try:
                i.drop(bind=connection)
            except:
                pass

    # load TAS
    if tasFile:
        filename = tasFile
    else:
        filename = "../config/all_tas_betc.csv"
    try:
       TASLoader.loadFields(filename)
    except IOError:
       print("Can't open file: {}".format(filename))
       raise

    # re-create indexes
    if dropIdx:
        for i in indexes:
            i.create(bind=connection)

if __name__ == '__main__':
    loadTas()