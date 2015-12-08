from elementtree.ElementTree import Element, SubElement
from _elementtree import ElementTree

class SchemaChecker:
    """ This class will load a schema file and writes the set of validation rules to validation database.

    Instance fields:
    columnInfo -- dict of dicts (columnInfo), main key is column name, value dicts have keys: "required" (True or False), "type", "criteria string"
    columnNames -- array of column names, used to map position in csv to columnInfo, can be redefined for each file based on first row
    """
def __init__(self,schemaFileName):
    """ Load schema file to create validation rules

    Arguments:
    schemaFileName -- filename of xml file that holds schema definition
    """
    # Load XML schema file
    tree = ElementTree.parse("awardSchema.xml")
    root = tree.getRoot()

    # Based on first row create dicts for columnInfo
    # Read rest of rows into columnInfo
    pass

def check(self, dataFileName):
    """ Check all data in dataFileName against preloaded schema

    Arguments:
    dataFileName -- filename of data file to load and check
    Returns:
    Either True for success or some error message
    """




# StagingTableCreator will use schemaLoader and then:
#   For each dict in columnInfo, create a column in sqlalchemy table object
#   Create table in DB
