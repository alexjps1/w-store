from lstore.index import Index
from lstore.placeholder_index import DumbIndex
from time import time
from lstore.page import Page
from typing import List, Literal
from lstore.config import RID_COLUMN, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN, NUM_METADATA_COLUMNS, RID_TOMBSTONE_VALUE, CUMULATIVE_TAIL_RECORDS
from lstore.config import schema_AND, schema_SUBTRACT, bytearray_to_int, int_to_bytearray, rid_to_coords, coords_to_rid, schema_to_bytearray, bytearray_to_schema
# graphing
from lstore.config import FIXED_PARTIAL_RECORD_SIZE, PAGE_SIZE, INDEX_USE_BPLUS_TREE, OVERRIDE_WITH_DUMB_INDEX, INDEX_BPLUS_TREE_MAX_DEGREE

def debug_print(debug_rid, table):
    x1, x2, x3 = rid_to_coords(debug_rid)
    debug_cols = [table.get_partial_record(debug_rid, debug_index) for debug_index in range(len(table.page_directory.keys()))]
    print(debug_cols)

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
    def __init__(self, name:str, num_columns:int, key:int,
                 cumulative_tails:bool=CUMULATIVE_TAIL_RECORDS,
                 page_size=PAGE_SIZE,
                 record_size=FIXED_PARTIAL_RECORD_SIZE,
                 use_bplus=INDEX_USE_BPLUS_TREE,
                 use_dumbindex=OVERRIDE_WITH_DUMB_INDEX,
                 bplus_degree=INDEX_BPLUS_TREE_MAX_DEGREE):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # assert self.num_columns + NUM_METADATA_COLUMNS <= MAX_COLUMNS # max column is only limited by system storage?
        self.cumulative_tails = cumulative_tails
        self.page_size = page_size
        self.record_size = record_size
        self.use_bplus=use_bplus
        self.use_dumbindex=use_dumbindex
        self.bplus_degree=bplus_degree
        # add metadata columns
        self.page_directory = {
                               RID_COLUMN : {"base":[ Page(self.page_size, self.record_size) ], "tail":[]},
                                INDIRECTION_COLUMN : {"base":[ Page(self.page_size, self.record_size) ], "tail":[]},                #all pages associated with base column
                                SCHEMA_ENCODING_COLUMN : {"base":[ Page(self.page_size, self.record_size) ], "tail":[]},
                                TIMESTAMP_COLUMN : {"base":[ Page(self.page_size, self.record_size) ], "tail":[]},
                                }
        # add data columns
        for i in range(num_columns):
            self.page_directory[NUM_METADATA_COLUMNS + i] = {"base":[ Page(self.page_size, self.record_size) ], "tail":[]}

        if not self.use_dumbindex:
            self.index = Index(self, use_bplus=self.use_bplus, degree=self.bplus_degree)
        else:
            self.index = DumbIndex(self)

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
        Appends a new tail record for the given column updates. This process is similar for cumulative and non-cumulative tail records. This is because the tail record RIDs require that the page entries for tail records line up. So even the non-cumulative implementation will require null values to be written to non-updated columns.
        Also assumes new base records default to their RID in both the RID_COLUMN and INDIRECTION_COLUMN.

        Inputs:
            - base_RID, the base record's RID
            - columns, the updates the tail record should contain, None values should be placed in columns without an update.
        Outputs:
            - True on a successful update, False otherwise
        """
        # append new tail record with *columns, and indirection to other tail record's RID
        # find the most recent tail record from base record's indirection
        # check tail != base, or that Base Records default to their RIDs in the INDIRECTION_COLUMN instead of a null value
        old_tail_rid = self.get_partial_record(base_RID, INDIRECTION_COLUMN)
        # print("base")
        # debug_print(base_RID, self)
        # print("old tail")
        # debug_print(old_tail_rid, self)
        # check if this record is deleted
        if old_tail_rid == RID_TOMBSTONE_VALUE:
            return False

        if self.cumulative_tails:
            # cumulative tail records store the current version of the record and no lookback is needed
            schema_encoding = [1]*len(columns)
            # set None values in columns to last record's values
            new_columns = [0]*len(columns)
            for i, value in enumerate(columns):
                if value is None:
                    # this part is unique to the cumulative records
                    new_columns[i] = self.get_partial_record(old_tail_rid, i + NUM_METADATA_COLUMNS)
                else:
                    new_columns[i] = columns[i]
            columns = new_columns
        else:
            schema_encoding = [1 if x is not None else 0 for x in columns]
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
        base_page.overwrite_direct(int_to_bytearray(new_tail_rid, self.record_size), offset)
        # print("new tail")
        # debug_print(new_tail_rid, self)
        return success_state

    def write_new_record(self, RID:int, indirection:int, schema:list[int], columns:list[int], rid_page:Page, record_type:Literal["tail"]|Literal["base"]) -> bool:
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
        # tail, _, _ = rid_to_coords(RID)
        # print("----Writing new record---- istail{}, rid{}, ind{}, schema{}, columns{}".format(int(tail), RID, indirection, schema, columns))
        timestamp = int(time()) # accurate to the second only
        # write the metadata columns
        write_cols:list[int] = [INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, TIMESTAMP_COLUMN]
        write_vals:list[bytearray] = [int_to_bytearray(indirection, self.record_size), schema_to_bytearray(schema, self.record_size), int_to_bytearray(timestamp, self.record_size)]
        # write RID
        rid_page.write_direct(int_to_bytearray(RID, self.record_size))
        # the rid page number is needed for RID generation, so it is redundant to include writing the rid in the for loop
        for i, col in enumerate(write_cols):
            page = self.get_writable_page(col, record_type)
            page.write_direct(write_vals[i])

        # write data columns
        for i, col in enumerate(columns):
            page = self.get_writable_page(i + NUM_METADATA_COLUMNS, record_type)
            if schema[i] or record_type == "base":
                # write data to page
                page.write_direct(int_to_bytearray(col, self.record_size))
                if record_type == "base":
                    # update index with RID, i, and col
                    self.index.add_record_to_index(i, col, RID)
            else:
                # write a None value, it should be skipped by the schema encoding when read
                page.write_direct(int_to_bytearray(0, self.record_size))
        # update was successful
        return True

    def get_writable_page(self, column:int, key:Literal["tail"]|Literal["base"]="tail") -> Page:
        """
        Obtains a tail/base page for the specified column with space for at least one write. NOTE if a new page needs to be allocated, a new page will be added for all columns, since the RIDs require pages to be lined up across columns.

        Inputs:
            - column, the column the page is part of
            - key, ether "tail" for tail records or "base" for base records
        Outputs:
            - the page object
        """
        add_new = False
        if len(self.page_directory[column][key]) != 0:
            page = self.page_directory[column][key][-1]
            if not page.has_capacity():
                # add a new page
                # print("\n######### {} Page Full #########\n".format(key))
                add_new = True
            else:
                return page
        else:
            # add the first new page
            # print("add first set of {} pages".format(key))
            add_new = True

        if add_new:
            # add new page for all columns
            for col in self.page_directory.keys():
                self.add_page(col, key)

        page = self.page_directory[column][key][-1]
        # return the Page
        return page

    def locate_record(self, RID: int, key:int, column_mask:list[int], version:int=0) -> Record|Literal[False]:
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
        tail, _, _ = rid_to_coords(tail_RID)
        if tail:
            if version == 0:
                # we are interested in the current version of the record
                pass # no extra action needed
            elif version < 0:
                # we are interested in a past version of the record
                # locate the correct version
                for _ in range(version, 0):
                    # get the next tail record
                    # debug_print(tail_RID, self)
                    tmp_tail_rid = tail_RID
                    tail_RID = self.get_partial_record(tail_RID, INDIRECTION_COLUMN)
                    if tail_RID == RID:
                        # if history stack is smaller then disiered result, give the base rid
                        record = Record(RID, key, [self.get_partial_record(RID, i + NUM_METADATA_COLUMNS) for i in range(len(column_mask))])
                        return record
                # tail_RID is now the RID of the -version tail record
            else: # version > 0:
                #NOTE this will be treated the same as version == 0
                pass
            # apply the tail records to base record and return columns as directed
            record = self.apply_tails_to_base(tail_RID, RID, key, column_mask)
            # print("Found Tail records for base rid{}, key{}, columns{}".format(RID, key, record.columns))
            return record

        # return base RID if no tail records
        record = Record(RID, key, [self.get_partial_record(RID, i + NUM_METADATA_COLUMNS) if value else None for i, value in enumerate(column_mask)])
        # print("Found Base record rid{}, key{}, columns{}".format(RID, key, record.columns))
        return record

    def apply_tails_to_base(self, Tail_RID:int, Base_RID:int, key:int, column_mask:list[int]) -> Record:
        """
        Calculates the record represented by the given tail record. Works for both cumulative and non-cumulative tail records as well as any past version.

        Inputs:
            - Tail_RID, the RID of the most recent tail record for this version
            - Base_RID, the RID of the record's base record
            - key, the column where the primary key is located
            - column_mask, essentially the schema encoding for what columns to return the results for
        Outputs:
            - a Record object representing the record at the desired version
        """
        # go through tail records until all columns in column_mask have been found in schema, or base record is hit
        # columns may not be added in order, and some may not be in the tail records
        columns = [None]*len(column_mask)
        working_mask = column_mask
        aggregate_mask = column_mask
        # print("all masks at agg {} work {} col {}".format(aggregate_mask, working_mask, column_mask))
        tail_rid = Tail_RID
        # NOTE cumulative and non-cumulative implementations are functionally similar in this code base since records must be lined up along pages, non-cumulative tail records will write null values to pages not in the update range, but the schema_encoding should prevent those partial records from being read.

        # print("base record")
        # debug_print(Base_RID)

        while 1 in aggregate_mask:
            is_tail, _, _ = rid_to_coords(tail_rid)
            # print("applying tail", tail_rid)
            # debug_print(tail_rid)
            if is_tail:
                tail_schema = self.get_partial_record(tail_rid, SCHEMA_ENCODING_COLUMN)
                # only check columns that are the intersection of the tail record schema and the column_mask
                # print("0.0 masks at {} AND {}".format(aggregate_mask, tail_schema))
                working_mask = schema_AND(aggregate_mask, tail_schema)
                # print("0.1 aggregate_mask at {}".format(aggregate_mask))
                # add each column to result if it is both part of the tail record's schema and the column_mask
                for i, mask in enumerate(working_mask):
                    if mask:
                        columns[i] = self.get_partial_record(tail_rid, i + NUM_METADATA_COLUMNS)
                        # print("add value", columns[i], "at", i)
                # subtract schema from aggregate_mask, since we have added those values to the result
                # print("1.0 masks at {} AND {}".format(aggregate_mask, tail_schema))
                aggregate_mask = schema_SUBTRACT(aggregate_mask, tail_schema)
                # print("1.1 aggregate_mask at {}".format(aggregate_mask))
                # move to next tail record
                tail_rid = self.get_partial_record(tail_rid, INDIRECTION_COLUMN)
                # check if we are at the base record
                # TODO it may be better if we check the RID itself
                if tail_rid == Base_RID:
                    break # we are done
            else:
                break
        # apply base record values for all schema still in working_mask
        # print("f aggregate_mask at {}".format(aggregate_mask))
        for i, mask in enumerate(aggregate_mask):
            if mask:
                columns[i] = self.get_partial_record(Base_RID, i + NUM_METADATA_COLUMNS)

        # print(Base_RID, key, columns)
        return Record(Base_RID, key, columns)

    def get_partial_record(self, RID:int, column:int) -> list[int]|int:
        """
        Accepts the RID and the column number and returns the partial record contained at the correct page and offset.
        NOTE: should not be used directly for any data column as it will not apply the tail records to the result.

        Inputs:
            RID, the record id
            column: the column index of interest
        Outputs:
            the partial record data: bytearray representing an int or list of 0 and 1 for schema
        """
        # get page number and offset
        tail, page_num, offset = rid_to_coords(RID)
        # get raw data
        if tail:
            data = self.page_directory[column]["tail"][page_num].retrieve_direct(offset)
        else:
            data = self.page_directory[column]["base"][page_num].retrieve_direct(offset)

        if column == SCHEMA_ENCODING_COLUMN:
            data = bytearray_to_schema(data, self.num_columns)
        else:
            data = bytearray_to_int(data)
        return data

    def add_page(self, col_number:int, page_type:Literal["base"]|Literal["tail"]) -> None:
        """
        Adds a page when number of records exceeds page size

        INPUTS
            - col_number, The column that is being extended
            - page_type, ether "base" for base pages, or "tail" for tail pages
        Outputs:
            - None, the page will be added to the end of the page_directory
        """
        page = Page(self.page_size, self.record_size)
        self.page_directory[col_number][page_type].append(page)

    def delete_record(self, base_RID:int) -> bool:
        """
        Deletes the record with the given RID by setting its base RID to the Tombstone RID.

        Inputs:
            - base_RID, the RID of the base record that is to be deleted
        Outputs:
            - True if the record was deleted, False otherwise
        """
        # set the INDIRECTION_COLUMN of the base record to a tombstone value
        # get page number and offset
        tail, page_num, offset = rid_to_coords(base_RID)
        if not tail:
            self.delete_record_from_index(base_RID)
        page = self.page_directory[INDIRECTION_COLUMN]["base"][page_num]
        page.overwrite_direct(int_to_bytearray(RID_TOMBSTONE_VALUE, self.record_size), offset)
        return True

    def delete_record_from_index(self, base_RID:int) -> None:
        """
        Helper function for removing an entire record from the index
        """
        for i in range(self.num_columns):
            value = self.get_partial_record(base_RID, i + NUM_METADATA_COLUMNS)
            self.index.remove_record_from_index(i, value, base_RID)

    def __merge(self):
        print("merge is happening")
        pass
