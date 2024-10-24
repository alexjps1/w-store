from lstore.table import Table, Record
# from lstore.index import Index
from typing import Literal
from lstore.config import NUM_METADATA_COLUMNS


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key:int) -> bool:
        # find the record
        rid = self.table.index.locate(0, primary_key)
        # delete record and return success state
        return self.table.delete_record(rid)
    
    
    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        return self.table.insert_record_into_pages(columns)

    
    """
    # Finds all matching records with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values (i.e. [0, 0, 1, 1]).
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key:int, search_key_index:int, projected_columns_index:list[bool]) -> list[Record]|Literal[False]:
        # find the Record IDs
        rids = self.table.index.locate(search_key_index, search_key)
        # get relevant columns for the records
        records = [self.table.locate_record(rid, search_key, projected_columns_index, 0) for rid in rids]
        return records

    
    """
    # Finds all matching records with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retrieve.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # find the Record IDs
        rids = self.table.index.locate(search_key_index, search_key)
        # get relevant columns for the records
        records = [self.table.locate_record(rid, search_key, projected_columns_index, relative_version) for rid in rids]
        return records

    
    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # find the base record with primary_key
        # TODO return False if index fails to find rid
        try:
            rid = self.table.index.locate(NUM_METADATA_COLUMNS, primary_key)[0]
        except IndexError:
            return False
        # append new tail record with *columns, and indirection to other tail record's RID and set base record's indirection to new tail's RID
        return self.table.append_tail_record(rid, columns)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range:int, end_range:int, aggregate_column_index:int) -> int|Literal[False]:
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retrieve.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range:int, end_range:int, aggregate_column_index:int, relative_version:int) -> int|Literal[False]:
        # ask index to find the relevant RIDs
        # TODO return False if no records where found
        rid_set = self.table.index.locate_range(start_range, end_range, aggregate_column_index)
        # get the attribute values and return the sum
        record_list = [None]*len(rid_set) # preallocate an empty list
        # build a all 0 column mask except for the aggregate column
        column_mask = [0]*self.table.num_columns
        column_mask[aggregate_column_index] = 1
        sum_value = 0
        # sum records after applying tails
        for rid in rid_set:
            record = self.table.locate_record(rid, 0, column_mask, relative_version)
            sum_value += record.columns[0]
        return sum_value

    
    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
