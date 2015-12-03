import os, sys, inspect
# Add all needed subdirectories to path if they are not there yet
filePath = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
neededSubDirs = ["","/handlers","/models","/handlers/aws","/handlers/utils"]
for subDir in neededSubDirs:
    pathToAdd =  filePath + subDir
    if(not(pathToAdd in sys.path)):
        sys.path.append(pathToAdd)