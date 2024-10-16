from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    """
    Instantiates a record object
    
    INPUTS
        -rid            int             #Record ID
        -key            int             #index or col_number?
        -columns        list[ints]      #list of values in each column"""
    def __init__(self, rid:int, key:int, columns:list[int]):
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
    def __init__(self, name:str, num_columns:int, key:int):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = { INDIRECTION_COLUMN : [ Page() ],                #all pages associated with base column
                                RID_COLUMN : [ Page() ],
                                TIMESTAMP_COLUMN : [ Page() ],
                                SCHEMA_ENCODING_COLUMN : [ Page() ]
                                }
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
 
