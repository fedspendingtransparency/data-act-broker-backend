from dataactvalidator.filestreaming.tasLoader import TASLoader
from setupTASIndexs import setupTASIndexs

def loadTas():
    """ Load all valid TAS combinations into database and index the TASLookup table """
    filename = "../../tests/all_tas_betc.csv"
    TASLoader.loadFields(filename)

    setupTASIndexs()

if __name__ == '__main__':
    loadTas()