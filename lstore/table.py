from lstore.index import Index
from time import time
from lstore.page import Page
from typing import List
from lstore.config import RID_COLUMN, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN, MAX_COLUMNS


class Record:
    """
    Instantiates a record object

    INPUTS
        -rid            int             #Record ID
        -key            int             #index or col_number?
        -columns        list[ints]      #list of values in each column"""
    def __init__(self, rid: int, key: int, columns: List[int]):
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
        assert self.num_columns <= MAX_COLUMNS
        # add metadata columns
        self.page_directory = { 
                                RID_COLUMN : [ Page() ],
                                INDIRECTION_COLUMN : [ Page() ],                #all pages associated with base column
                                SCHEMA_ENCODING_COLUMN : [ Page() ],
                                TIMESTAMP_COLUMN : [ Page() ],
                                }
        # add data columns
        for i in range(num_columns):
            self.page_directory[4 + i] = [ Page() ]

        self.index = Index(self)
        pass

    def insert_record_into_pages(self, schema, *columns):
        pass

    def locate_record(self, RID: int) -> List[Record]:

        """
        Given the RID, provides the records with that RID via using
        indexing to find their row number.

        INPUTS:
            -RID:          int        #The record Id
        OUTPUT:
            The page the record is stored in, tupled with byte offset
        """
        record_idxs = self.index.locate(1, RID)
        records = []
        for i in record_idxs:
            values = []
            for col in self.num_columns:
                values.append(col[i])                   #Assumes list implementation, we use bytearrays
            records.append(Record(RID, ___, list))      #What goes in key section?


        return records

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
