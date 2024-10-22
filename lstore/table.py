from lstore.index import Index
from time import time
from lstore.page import Page
from typing import List
from lstore.config import RID_COLUMN, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN, MAX_COLUMNS, NUM_METADATA_COLUMNS
from lstore.config import schema_AND, schema_SUBTRACT, bytearray_to_int, bytearray_to_schema, int_to_bytearray, schema_to_bytearray
from rid import rid_to_coords


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
            self.page_directory[NUM_METADATA_COLUMNS + i] = [ Page() ]

        self.index = Index(self)
        pass

    def insert_record_into_pages(self, schema, *columns):
        pass

    def locate_record(self, RID: int, key:int, column_mask:list[bool], version:int=0) -> Record:
        """
        Given the RID, provides the record with that RID via indexing.

        INPUTS:
            RID: int, the record id
            key: int, this is the key needed by the Record class
            column_mask: list[bool], which columns the record should contain
            version: int, the relative version of the record to locate. 0 is the base record, negative numbers indicate tail records.
        OUTPUT:
            Record object
        """
        # get the base record's indirection column (the first tail record's RID)
        tail_RID = self.get_partial_record(RID, INDIRECTION_COLUMN)
        if version == 0:
            # we are interested in the current version of the record
            pass # no extra action needed
        elif version < 0:
            # we are interested in a past version of the record
            # locate the correct version
            for _ in range(version, 0): #TODO check this does not miss one
                # get the next tail record
                tail_RID = self.get_partial_record(tail_RID, INDIRECTION_COLUMN)
            # tail_RID is now the RID of the -version tail record
        else: # version > 0:
            #NOTE this will be treated the same as version == 0
            pass
        # apply the tail records to base record and return columns as directed
        return self.apply_tails_to_base(tail_RID, RID, key, column_mask)

    def apply_tails_to_base(self, Tail_RID:int, Base_RID:int, key:int, column_mask:list[bool]) -> Record:
        """
        Calculates the record represented by the given tail record.
        """
        # go through tail records until all columns in column_mask have been found in schema, or base record is hit
        # columns may not be added in order, and some may not be in the tail records
        columns = [None]*len(column_mask)
        working_mask = column_mask
        tail_rid = Tail_RID
        while True in working_mask:
            tail_schema = self.get_partial_record(tail_rid, SCHEMA_ENCODING_COLUMN)
            # only check columns that are the intersection of the tail record schema and the column_mask
            working_mask = schema_AND(working_mask, tail_schema)
            # add each column to result if it is both part of the tail record's schema and the column_mask
            for i, mask in enumerate(working_mask):
                if mask:
                    columns[i] = self.get_partial_record(tail_rid, i + NUM_METADATA_COLUMNS)
            # subtract schema from column_mask, since we have added those values to the result
            working_mask = schema_SUBTRACT(column_mask, tail_schema)
            # move to next tail record
            tail_rid = self.get_partial_record(tail_rid, INDIRECTION_COLUMN)
            # check if we are at the base record
            # TODO it may be better if we check the RID itself
            if tail_rid == Base_RID:
                break # we are done
        # apply base record values for all schema still in working_mask
        for i, mask in enumerate(working_mask):
            if mask:
                columns[i] = self.get_partial_record(tail_rid, i + NUM_METADATA_COLUMNS)

        # trim out unassigned columns
        columns = [col for col in columns if col is not None]
        return Record(Base_RID, key, columns)

    def locate_base_record(self, RID:int, key:int, column_mask:list[bool]) -> Record:
        # for given column_mask:
        columns = []
        for i, mask in enumerate(column_mask):
            if mask:
                columns.append(self.get_partial_record(RID, i + NUM_METADATA_COLUMNS))
        # build the record object
        return Record(RID, key, columns)

    def get_partial_record(self, RID:int, column:int) -> int|list[bool]:
        """
        Accepts the RID and the column number and returns the partial record contained at the correct page and offset.
        
        Inputs:
            RID, the record id
            column: the column index of interest
        Outputs:
            the partial record data as an int or list of 0 and 1 for schema
        """
        # get RID and offset
        tail, page_num, offset = rid_to_coords(RID)
        # get raw data
        data = self.page_directory[column][page_num].retrieve_direct(offset)
        # convert from bytearray
        if column == SCHEMA_ENCODING_COLUMN:
            # process data as schema
            return bytearray_to_schema(data)
        # elif column == TIMESTAMP_COLUMN:
        #     # process data as time
        #     pass
        else:
            # process data as int
            # use for data columns, RIDs, and Indirection, TODO also timestamp?
            return bytearray_to_int(data)


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
