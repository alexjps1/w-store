from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:
    """
    Instantiates a table object

    INPUTS:
        -name:          string            #Table name
        -num_columns:   int               #Number of Columns: all columns are integer
        -key:           int               #Index of table key in columns
    OUTPUT:
        -table object
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        pass

    def locate_record(self, RID:int):
        """
        Given the RID, locates the physical location of the
        corresponding record.

        INPUTS:
            -RID:          int        #The record Id
        OUTPUT:
            The page the record is stored in, tupled with byte offset"""
        pass

    def add_page(self, col_number):
        """
        Adds a page when number of records exceeds page size

        INPUTS
            -col_number     int         The column that is being extended
        """
        pass



    def __merge(self):
        print("merge is happening")
        pass
 
