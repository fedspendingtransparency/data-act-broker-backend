""" Add all needed subdirectories to path if they are not there yet """

import os, sys, inspect

filePath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
neededSubDirs = ["data-act-core/aws","data-act-core/models","data-act-core/utils"]
for subDir in neededSubDirs:
    pathToAdd =  filePath + subDir
    if(not(pathToAdd in sys.path)):
        sys.path.append(pathToAdd)
