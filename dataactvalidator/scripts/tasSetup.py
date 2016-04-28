from dataactvalidator.filestreaming.tasLoader import TASLoader
from dataactvalidator.scripts.setupTASIndexs import setupTASIndexs

def loadTAS(filename):
    """ Load TAS combinations from specified file """
    TASLoader.loadFields(filename)
    setupTASIndexs()
