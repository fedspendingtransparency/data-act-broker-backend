from filestreaming.tasLoader import TASLoader
from setupTASIndexs import setupTASIndexs

def loadTas():
    filename = "all_tas_betc.csv"
    TASLoader.loadFields(filename)

    setupTASIndexs()

if __name__ == '__main__':
    loadTas()