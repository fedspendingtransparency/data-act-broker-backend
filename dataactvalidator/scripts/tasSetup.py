from dataactvalidator.filestreaming.tasLoader import TASLoader
from dataactvalidator.scripts.setupTASIndexs import setupTASIndexs

def loadTAS(filename):
    TASLoader.loadFields(filename)
    setupTASIndexs()
