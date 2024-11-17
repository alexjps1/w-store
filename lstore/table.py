from lstore.index import Index
from lstore.placeholder_index import DumbIndex
from lstore.page_directory import PageDirectory
from time import time_ns
from lstore.page import Page
from typing import List, Literal, Tuple
from pathlib import Path
from lstore.config import *
import copy

# graphing
# from lstore.config import FIXED_PARTIAL_RECORD_SIZE, PAGE_SIZE, INDEX_USE_BPLUS_TREE, OVERRIDE_WITH_DUMB_INDEX, INDEX_BPLUS_TREE_MAX_DEGREE


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
    def __init__(self, name:str, database_name:Path, num_columns:int, key:int,
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
        self.ref_time = time_ns()
        # add metadata columns
        self.metadata_cols = [RID_COLUMN, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN, CREATED_TIME_COLUMN, UPDATED_TIME_COLUMN]
        self.page_directory = PageDirectory(name, database_name)
        # get current base and tail page numbers
        # index of the current base pages that are not full
        self.current_base_page_number = self.page_directory.file_manager.get_page_number(False)
        # index of the current tail pages that are not full, start at -1 since we do not start with any tail pages allocated
        self.current_tail_page_number = self.page_directory.file_manager.get_page_number(True)
        # print(f"current page #:: B:{self.current_base_page_number}, T:{self.current_tail_page_number}")
        if self.current_base_page_number == -1:
            # initialize a new empty table
            self.current_base_page_number = 0
            for col in self.metadata_cols:
                self.page_directory.insert_page(Page(self.page_size, self.record_size),
                                                col,
                                                False,
                                                self.current_base_page_number)
            # add data columns
            for i in range(num_columns):
                self.page_directory.insert_page(Page(self.page_size, self.record_size),
                                                NUM_METADATA_COLUMNS + i,
                                                False,
                                                self.current_base_page_number)

        if not self.use_dumbindex:
            self.index = Index(self, use_bplus=self.use_bplus, degree=self.bplus_degree)
        else:
            self.index = DumbIndex(self)

        # attributes for merge algorithm
        self.merge_set: set = set()
        self.update_counter: int = 0

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
        page = self.get_writable_page(RID_COLUMN, False)
        # get info for new rid
        offset = page.num_records
        page_num = self.current_base_page_number
        # create the new rid
        new_rid = coords_to_rid(False, page_num, offset)
        # print(f"base RID{new_rid} page{page} page#{page_num} offset{offset}")
        # write metadata, put RID in both RID_COLUMN and INDIRECTION_COLUMN
        # write metadata and data columns
        success_state = self.write_new_record(new_rid, new_rid, [0]*self.num_columns, columns, page, False)
        return success_state

    def append_tail_record(self, base_RID:int, columns:list[int]) -> bool:
        """
        Appends a new tail record for the given column updates. None values will be skipped.
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
        page = self.get_writable_page(RID_COLUMN, True)
        # get info for new rid
        offset = page.num_records
        page_num = self.current_tail_page_number
        # create the new rid
        new_tail_rid = coords_to_rid(True, page_num, offset)
        # print(f"tail RID{new_tail_rid} page{page} page#{page_num} offset{offset}")
        # write metadata and data columns
        success_state = self.write_new_record(new_tail_rid, old_tail_rid, schema_encoding, columns, page, True, base_RID)

        # set base record's indirection to new tail's RID
        _, page_num, offset = rid_to_coords(base_RID)
        base_page = self.page_directory.retrieve_page(INDIRECTION_COLUMN, False, page_num)

        # mark page info (its page num and col num) for merging since we've just updated it
        # sidenote: the column numbers here are relative to data columns, i.e. 0 is the first data column
        # for col_num in (num for num, col_val in enumerate(columns) if col_val is not None):
        #     self.__add_to_merge_set((page_num, col_num))

        assert base_page is not None
        base_page.overwrite_direct(int_to_bytearray(new_tail_rid, self.record_size), offset)
        # print("new tail")
        # debug_print(new_tail_rid, self)
        return success_state

    def write_new_record(self, RID:int, indirection:int, schema:list[int], columns:list[int], rid_page:Page, is_tail:bool, base_rid:int=0) -> bool:
        """
        Helper function for writing a new record

        Inputs:
            - RID, the new record's RID
            - indirection, what should be put in the new record's INDIRECTION_COLUMN
            - schema, the schema encoding for the new record, for base pages this is all 0's
            - columns, the data columns to insert
            - rid_page, a reference to the RID page for the new record, this is needed to build the RID, so passing it into this function saves looking it up again
            - is_tail, ether True for tail records or False for base records
        Outputs:
            - True on a successful write, False otherwise
        """
        # tail, _, _ = rid_to_coords(RID)
        # print("----Writing new record---- istail{}, rid{}, ind{}, schema{}, columns{}".format(int(tail), RID, indirection, schema, columns))
        timestamp = time_ns() - self.ref_time # relative time since table was created, though 64 bit ints can store the full time anyway
        # print(f"## WRITE:: RID{RID}, col{columns}")
        # write the metadata columns
        write_cols:list[int] = self.metadata_cols[1:]
        write_vals:list[bytearray] = [int_to_bytearray(indirection, self.record_size), schema_to_bytearray(schema, self.record_size), int_to_bytearray(timestamp, self.record_size)]
        # write RID
        rid_page.write_direct(int_to_bytearray(RID, self.record_size))
        # the rid page number is needed for RID generation, so it is redundant to include writing the rid in the for loop
        for col_num, val_at_col in zip(write_cols, write_vals):
            page = self.get_writable_page(col_num, is_tail)
            page.write_direct(val_at_col)

        # write data columns
        for i, col in enumerate(columns):
            page = self.get_writable_page(i + NUM_METADATA_COLUMNS, is_tail)
            if schema[i] or not is_tail:
                if not is_tail:
                    # update index with RID, i, and col
                    self.index.add_record_to_index(i, col, RID)
                elif not self.use_dumbindex:
                    # get old data
                    old_value = self.get_partial_record(indirection, i + NUM_METADATA_COLUMNS)
                    # update index entry with updated values
                    self.index.update_record_in_index(i, old_value, base_rid, col)
                # write data to page
                page.write_direct(int_to_bytearray(col, self.record_size))
            else:
                # write a None value, it should be skipped by the schema encoding when read
                page.write_direct(int_to_bytearray(0, self.record_size))
        # update was successful
        return True

    def get_writable_page(self, column:int, is_tail:bool=True) -> Page:
        """
        Obtains a tail/base page for the specified column with space for at least one write. NOTE that a new page needs to be allocated if any column is full.

        Inputs:
            - column, the column the page is part of
            - is_tail, ether True for tail records or False for base records
        Outputs:
            - the page object
        """
        add_new = False
        new_page = False
        if (not is_tail and self.current_base_page_number >= 0) or (is_tail and self.current_tail_page_number >= 0):
            # get the current base or tail page number
            if is_tail:
                current_page_number = self.current_tail_page_number
            else:
                current_page_number = self.current_base_page_number
            # try to get the page
            page = self.page_directory.retrieve_page(column, is_tail, current_page_number)
            if page is not None:
                if not page.has_capacity():
                    # page was found, but it was full
                    # print("\n######### {} Page {} Full col{} offset{} #########\n".format("Tail" if is_tail else "Base", page, column, page.num_records))
                    new_page = True
                    add_new = True
                else:
                    # print("--------- {} Page Not {} Full col{} offset{}---------".format("Tail" if is_tail else "Base", page, column, page.num_records))
                    # page was found and it was not full, return the page
                    return page
            else:
                # page was not found, make a new one
                # print("********* No Page *********")
                add_new = True
        else:
            # add the first new page, this is functionally the same as the "if not page.has_capacity()" block above,
            # as the -1 "page" is "full" and we need a new page
            # print("add first set of {} pages".format(key))
            # print("$$$$$$$$$ First page $$$$$$$$$")
            new_page = True
            add_new = True

        if add_new:
            if new_page:
                # print( column == RID_COLUMN)
                # some new pages are in the same page row, new page row only after a page is filled
                if is_tail:
                    # print("current_tail_page_number now ", self.current_tail_page_number + 1)
                    self.current_tail_page_number += 1
                else:
                    self.current_base_page_number += 1
            self.add_page(column, is_tail)

        # get the current base or tail page number
        if is_tail:
            current_page_number = self.current_tail_page_number
        else:
            current_page_number = self.current_base_page_number
        page = self.page_directory.retrieve_page(column, is_tail, current_page_number)
        assert page is not None
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
        data = self.page_directory.retrieve_page(column, tail, page_num).retrieve_direct(offset)

        if column == SCHEMA_ENCODING_COLUMN:
            data = bytearray_to_schema(data, self.num_columns)
        else:
            data = bytearray_to_int(data)
        return data

    def add_page(self, col_number:int, is_tail:bool) -> None:
        """
        Adds a page when number of records exceeds page size

        INPUTS
            - col_number, The column that is being extended
            - is_tail, ether False for base pages, or True for tail pages
        Outputs:
            - None, the page will be added to the end of the page_directory
        """
        current_page_number = self.current_base_page_number if not is_tail else self.current_tail_page_number
        page = Page(self.page_size, self.record_size)
        self.page_directory.insert_page(page, col_number, is_tail, current_page_number)

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
        page = self.page_directory.retrieve_page(INDIRECTION_COLUMN, False, page_num)
        page.overwrite_direct(int_to_bytearray(RID_TOMBSTONE_VALUE, self.record_size), offset)
        return True

    def delete_record_from_index(self, base_RID:int) -> None:
        """
        Helper function for removing an entire record from the index
        """
        for i in range(self.num_columns):
            value = self.get_partial_record(base_RID, i + NUM_METADATA_COLUMNS)
            self.index.remove_record_from_index(i, value, base_RID)

    ### Methods for merging ###

    def __increment_update_counter(self):
        """
        Increment the update counter by one.
        Later on, mutex lock functionality can be added here if needed.
        """
        # while self.update_counter_lock:
        #    pass
        # self.update_counter_lock = True
        self.update_counter += 1
        # self.update_counter_lock = False

    def __empty_merge_set(self) -> set:
        """
        Empty the merge set, returning a copy of it.
        Later on, mutex lock functionality can be added here if needed.
        """
        copied_merge_set = self.merge_set.copy()
        self.merge_set.clear()
        return copied_merge_set

    def __add_to_merge_set(self, page_col_tuple: Tuple[int, int]):
        """
        Add a (page_num, col_num) tuple into the merge set, ignoring duplicates.
        Also updates the update counter and calls merge if it's time to merge.
        Thus, this should be called on *every* update.
        Later on, mutex lock functoinality can be added here if needed.
        """
        self.__increment_update_counter()
        self.merge_set.add(page_col_tuple)
        if self.update_counter % NUM_UPDATES_TO_MERGE == 0:
            self.__merge()

    def __merge(self):
        """
        Warning: Does not work with non-cumulative tail records at this time
        """
        try:
            assert CUMULATIVE_TAIL_RECORDS
        except AssertionError:
            raise NotImplementedError("Backtracking through tail records for non-cumulative not yet implemented")

        # operate on entire merge set copy
        merge_set = self.__empty_merge_set()
        for page_num, col_num in merge_set:
            # get the base page and indirection page relevant to this merge
            base_page = self.page_directory.retrieve_page(col_num, False, page_num)
            indir_page = self.page_directory.retrieve_page(INDIRECTION_COLUMN, False, page_num)
            if not base_page or not indir_page:
                raise KeyError("Failed to fetch page in __merge()")

            # create a "consolidated" base page which will contain updated values
            cons_base_page = copy.copy(base_page)

            # DEBUG assertion
            assert base_page.num_records == PAGE_SIZE / FIXED_PARTIAL_RECORD_SIZE

            # replace each record in this base page with its newest value
            for offset in range(base_page.num_records):
                tail_record_rid = bytearray_to_int(indir_page.retrieve_direct(offset))
                if tail_record_rid == RID_TOMBSTONE_VALUE:
                    # this record has been deleted, nothing to update
                    continue
                _, tail_page_num, tail_record_offset = rid_to_coords(tail_record_rid)

                # DEBUG assertion
                assert _ is True

                # find the tail record and update the base record with its value
                tail_page = self.page_directory.retrieve_page(col_num, True, tail_page_num)
                if tail_page is None:
                    raise KeyError("Base record is not deleted but its indirection column points to a non-existent tail page")
                new_val = bytearray_to_int(tail_page.retrieve_direct(tail_record_offset))
                cons_base_page.overwrite_direct(int_to_bytearray(new_val, FIXED_PARTIAL_RECORD_SIZE), offset)

            # swap the cons base page with the original base page
            self.page_directory.swap_page(cons_base_page, col_num, False, page_num)

    """
    def __merge(self):
        # Merge committed tail pages.
        print("merge is happening")
        while True:
            if len(self.merge_queue) != 0:

                # batch tail page is a tuple of format page  #
                batch_tail_page = self.merge_queue.popleft()
                batch_cons_page = batch_tail_page.copy()

                # NOTE backtracking thru tail records for non-cumulative not yet implemented
                if not CUMULATIVE_TAIL_RECORDS:
                    raise NotImplementedError("Backtracking through tail records for non-cumulative not yet implemented")

                # loop through every tail page
                for i in range(batch_tail_page.size):
                    tail_page = batch_tail_page[i]

                    # loop through each record within the tail page
                    for j in range(tail_page.size):
                        record = tail_page[j]

                        rid = record.RID

                        # add to hash map, update new consolidated base page
                        if rid not in seen_updates:
                            seen_updates[rid] = True
                            batch_cons_page.update(rid, record)

                        if len(seen_updates) == len(tail_page):
                            # not compressing, just need to add new corresponding page to disk
                            persist(batch_cons_page)
                batch_base_page = batch_tail_page.get_base_page_ref()

                self.page_directory.batch_base_page = batch_cons_page
                # deallocate outdatedbase pages
                del batch_base_page
    """
