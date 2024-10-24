# from lstore.index import Index
from lstore.placeholder_index import DumbIndex
from time import time
from lstore.page import Page
from typing import List, Literal
from lstore.config import RID_COLUMN, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN, MAX_COLUMNS, NUM_METADATA_COLUMNS, RID_TOMBSTONE_VALUE
from lstore.config import schema_AND, schema_SUBTRACT, bytearray_to_int, int_to_bytearray, rid_to_coords, coords_to_rid, schema_to_bytearray, bytearray_to_schema


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
                               RID_COLUMN : {"base":[ Page() ], "tail":[]},
                                INDIRECTION_COLUMN : {"base":[ Page() ], "tail":[]},                #all pages associated with base column
                                SCHEMA_ENCODING_COLUMN : {"base":[ Page() ], "tail":[]},
                                TIMESTAMP_COLUMN : {"base":[ Page() ], "tail":[]},
                                }
        # add data columns
        for i in range(num_columns):
            self.page_directory[NUM_METADATA_COLUMNS + i] = {"base":[ Page() ], "tail":[]}

        # self.index = Index(self)
        self.index = DumbIndex(self)
        pass

    def insert_record_into_pages(self, columns:list[int]) -> bool:
        """
        Inserts a new base record into the database. INDIRECTION_COLUMN defaults to the new rid of the base page.

        Inputs:
            - columns, the data to write in the new record, must be the same length as the table's data columns
        Outputs:
            - returns True on a successful insert, False otherwise
        """
        # TODO return False on a failed insert
        # make new Base RID
        # get writable base page
        page = self.get_writable_page(RID_COLUMN, key="base")
        # get info for new rid
        offset = page.num_records
        page_num = self.page_directory[RID_COLUMN]["base"].index(page)
        # create the new rid
        new_rid = coords_to_rid(False, page_num, offset)
        # write metadata, put RID in both RID_COLUMN and INDIRECTION_COLUMN
        # write metadata and data columns
        success_state = self.write_new_record(new_rid, new_rid, [0]*self.num_columns, columns, page, "base")
        return success_state

    def append_tail_record(self, base_RID:int, columns:list[int]) -> bool:
        """
        Appends a new tail record for the given column updates. This assumes cumulative tail records. This is because the tail record RIDs require that the page entries for tail records line up. So even the non-cumulative implementation will require null values to be written to non-updated columns.
        Also assumes new base records default to their RID in both the RID_COLUMN and INDIRECTION_COLUMN.
        """
        # append new tail record with *columns, and indirection to other tail record's RID
        # find the most recent tail record from base record's indirection
        # check tail != base, or that Base Records default to their RIDs in the INDIRECTION_COLUMN instead of a null value
        old_tail_rid = self.get_partial_record(base_RID, INDIRECTION_COLUMN)
        # check if this record is deleted
        if old_tail_rid == RID_TOMBSTONE_VALUE:
            return False

        schema_encoding = schema_to_bytearray([1]*len(columns)) # NOTE this is wrong for incomplete updates
        # get the page to append the new tail record rid
        page = self.get_writable_page(RID_COLUMN)
        # get info for new rid
        offset = page.num_records
        page_num = self.page_directory[RID_COLUMN]["tail"].index(page)
        # create the new rid
        new_tail_rid = coords_to_rid(True, page_num, offset)
        # write metadata and data columns
        success_state = self.write_new_record(new_tail_rid, old_tail_rid, schema_encoding, columns, page, "tail")

        # set base record's indirection to new tail's RID
        _, page_num, offset = rid_to_coords(base_RID)
        base_page = self.page_directory[INDIRECTION_COLUMN]["base"][page_num]
        base_page.overwrite_direct(int_to_bytearray(new_tail_rid), offset)

        return success_state

    def write_new_record(self, RID:int|bytearray, indirection:int|bytearray, schema:list[bool]|list[int]|bytearray, columns:list[int], rid_page:Page, record_type:Literal["tail"]|Literal["base"]) -> bool:
        """
        Helper function for writing a new record

        Inputs:
            - RID, the new record's RID
            - indirection, what should be put in the new record's INDIRECTION_COLUMN
            - schema, the schema encoding for the new record, for base pages this is all 0's
            - columns, the data columns to insert
            - rid_page, a reference to the RID page for the new record, this is needed to build the RID, so passing it into this function saves looking it up again
            - record_type, ether "tail" for tail records or "base" for base records
        Outputs:
            - True on a successful write, False otherwise
        """
        # convert input rids to bytearrays
        if type(RID) == int:
            RID = int_to_bytearray(RID)
        if type(indirection) == int:
            indirection = int_to_bytearray(indirection)
        if type(schema) != bytearray:
            schema = schema_to_bytearray(schema)

        timestamp = int(time()) # accurate to the second only
        # write the metadata columns
        write_cols:list[int] = [INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN]
        write_vals:list[bytearray] = [indirection, schema, int_to_bytearray(timestamp)]
        # write RID
        rid_page.write_direct(RID) 
        # the rid page number is needed for RID generation, so it is redundant to include writing the rid in the for loop
        for i, col in enumerate(write_cols):
            page = self.get_writable_page(col, record_type)
            page.write_direct(write_vals[i])
        
        # write data columns
        for i, col in enumerate(columns):
            # write data to page
            page = self.get_writable_page(i + NUM_METADATA_COLUMNS, record_type)
            page.write_direct(int_to_bytearray(col))

        # update was successful
        return True

    def get_writable_page(self, column:int, key:Literal["tail"]|Literal["base"]="tail") -> Page:
        """
        Obtains a tail/base page for the specified column with space for at least one write. NOTE if a new page needs to be allocated, a new page will be added for all columns, since the RIDs require pages to be lined up across columns.

        Input: column, the column the page is part of
        Output: the page object
        """
        add_new = False
        if len(self.page_directory[column][key]) != 0:
            page = self.page_directory[column][key][-1]
            if not page.has_capacity():
                # add a new page
                add_new = True
            else:
                return page
        else:
            # add the first new page
            add_new = True

        if add_new:
            # add new page for all columns
            for col in self.page_directory.keys():
                self.add_page(col, is_tail_page=(key=="tail"))
        
        page = self.page_directory[column][key][-1]
        # return the Page 
        return page

    def locate_record(self, RID: int, key:int, column_mask:list[bool]|list[int]|bytearray, version:int=0) -> Record|Literal[False]:
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
        # check if this record is deleted
        if tail_RID == RID_TOMBSTONE_VALUE:
            return False
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

    def apply_tails_to_base(self, Tail_RID:int, Base_RID:int, key:int, column_mask:list[bool]|bytearray) -> Record:
        """
        Calculates the record represented by the given tail record. Using non-cumulative tail records.
        """
        # go through tail records until all columns in column_mask have been found in schema, or base record is hit
        # columns may not be added in order, and some may not be in the tail records
        columns = bytearray(len(column_mask))
        working_mask = column_mask
        tail_rid = Tail_RID
        while True in working_mask and tail_rid != Base_RID:
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

    def get_partial_record(self, RID:int|bytearray, column:int) -> bytearray|list[int]:
        """
        Accepts the RID and the column number and returns the partial record contained at the correct page and offset.
        NOTE: should not be used directly for any data column as it will not apply the tail records to the result.
        
        Inputs:
            RID, the record id
            column: the column index of interest
        Outputs:
            the partial record data: bytearray representing an int or list of 0 and 1 for schema
        """
        if type(RID) == bytearray:
            rid = bytearray_to_int(RID)
        else:
            rid = RID
        # get page number and offset
        tail, page_num, offset = rid_to_coords(rid)
        # get raw data
        if tail:
            data = self.page_directory[column]["tail"][page_num].retrieve_direct(offset)
        else:
            data = self.page_directory[column]["base"][page_num].retrieve_direct(offset)

        if column == SCHEMA_ENCODING_COLUMN:
            data = bytearray_to_schema(data, self.num_columns)
        return data

    def add_page(self, col_number:int, is_tail_page:bool=False):
        """
        Adds a page when number of records exceeds page size

        INPUTS
            -col_number     int         The column that is being extended
        """
        if is_tail_page:
            page_type = "tail"
        else:
            page_type = "base"
        self.page_directory[col_number][page_type].append(Page())

    def delete_record(self, base_RID:int) -> bool:
        # set the INDIRECTION_COLUMN of the base record to a tombstone value
        # get page number and offset
        tail, page_num, offset = rid_to_coords(base_RID)
        page = self.page_directory[INDIRECTION_COLUMN]["base"][page_num]
        page.overwrite_direct(int_to_bytearray(RID_TOMBSTONE_VALUE), offset)
        pass

    def __merge(self):
        print("merge is happening")
        pass
