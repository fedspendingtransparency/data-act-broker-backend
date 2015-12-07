""" Add all needed subdirectories to path if they are not there yet """

import os, sys, inspect

filePath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
neededSubDirs = ["","/handlers","/models","/handlers/aws","/handlers/utils","/test"]
for subDir in neededSubDirs:
    pathToAdd =  filePath + subDir
    if(not(pathToAdd in sys.path)):
        sys.path.append(pathToAdd)