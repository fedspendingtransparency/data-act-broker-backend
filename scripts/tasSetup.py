from filestreaming.tasLoader import TASLoader
from scripts.setupTASIndexs import setupTASIndexs

def loadTAS(filename):
    TASLoader.loadFields(filename)
    setupTASIndexs()
