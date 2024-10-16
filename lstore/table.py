from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
TAIL_INDIRECTION_COLUMN = 4
TAIL_RID_COLUMN = 5
TAIL_TIMESTAMP_COLUMN = 6
TAIL_SCHEMA_ENCODING_COLUMN = 7


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
                                SCHEMA_ENCODING_COLUMN : [ Page() ],
                                TAIL_INDIRECTION_COLUMN : [ Page() ],           #Tail columns also interpreted as pages
                                TAIL_RID_COLUMN : [ Page() ],
                                TAIL_TIMESTAMP_COLUMN : [ Page() ],
                                TAIL_SCHEMA_ENCODING_COLUMN : [ Page() ],
                                }
        self.index = Index(self)
        pass

    def locate_record(self, RID:int)->list[Record]:
        """
        Given the RID, provides the records with that RID via using
        indexing to find their row number.

        INPUTS:
            -RID:          int        #The record Id
        OUTPUT:
            The page the record is stored in, tupled with byte offset"""
        record_idxs = self.index.locate(1, RID)
        records = []
        for i in record_idxs:
            values = []
            for col in self.num_columns:
                values.append(col[i])                   #Assumes list implementation, we use bytearrays
            records.append(Record(RID, ___, list))      #What goes in key section?
        pass

    def add_page(self, col_number):
        """
        Adds a page when number of records exceeds page size

        INPUTS
            -col_number     int         The column that is being extended
        """
        self.page_directory[col_number].append(Page())
        pass



    def __merge(self):
        print("merge is happening")
        pass
 
