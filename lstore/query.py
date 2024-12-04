from lstore.table import Table, Record
from lstore.config import debug_print as print
from typing import Literal
import traceback


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key:int) -> bool:
        try:
            # find the record
            rid = self.table.index.locate(self.table.key, primary_key)[0]
            # delete record and return success state
            value = self.table.delete_record(rid)
            return value
        except Exception as e:
            print(f"Exception in delete :: {e} {traceback.format_exc()}")
            return False

    """
    # Insert a record with specified columns
    # Return True upon successful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns) -> bool:
        if len(columns) != self.table.num_columns:
            return False
        try:
            primary_key_col = self.table.key
            column_mask = [x==primary_key_col for x in range(len(columns))]
            new_primary_key = columns[primary_key_col]

            # check new_primary_key is not None
            if new_primary_key is None:
                # can't insert without a primary key
                return False
            # don't insert all None
            all_none = [x is None for x in columns]
            if False in all_none:
                pass
            else:
                # all values in columns are None
                return False

            existing_primary_key = self.select(new_primary_key, primary_key_col, column_mask)
            if len(existing_primary_key) == 0:
                # primary key does not exist
                value = self.table.insert_record_into_pages(columns)
                # print(f"value of insert :: {value}")
                return value
            else:
                # print(f"existing primary key on insert")
                # don't add existing primary keys
                return False
        except Exception as e:
            print(f"Exception in insert :: {e} {traceback.format_exc()}")
            return False


    """
    # Finds all matching records with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values (i.e. [0, 0, 1, 1]).
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key:int, search_key_index:int, projected_columns_index:list[bool]) -> list[Record]:
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)



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
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version) -> list[Record]:
        if len(projected_columns_index) != self.table.num_columns:
            return []
        try:
            # find the Record IDs
            rids = self.table.index.locate_version(search_key_index, search_key, relative_version)
            if rids is False or len(rids) == 0:
                return [] 
            # get relevant columns for the records
            records = [self.table.locate_record(rid, search_key, projected_columns_index, relative_version) for rid in rids]
            return records
        except Exception as e:
            print(f"Exception in select_version :: {e} {traceback.format_exc()}")
            return []

    """
    # Update a record with specified key and columns
    # Returns True if update is successful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns) -> bool:
        if len(columns) != self.table.num_columns:
            # don't allow updates without correct number of columns
            return False
        try:
            # don't insert all None
            all_none = [x is None for x in columns]
            if False in all_none:
                pass
            else:
                # all values in columns are None
                return False

            # find the base record with primary_key
            rids = self.table.index.locate(self.table.key, primary_key)
            if rids is None or rids is False or len(rids) == 0:
                return False
            else:
                rid = rids[0]
            new_primary_key = columns[self.table.key]
            # print(f"rids :: {rid}, new_primary_key :: {new_primary_key}")
            if new_primary_key is not None:
                # check this new primary key is not in the table
                primary_key_col = self.table.key
                column_mask = [x==primary_key_col for x in range(len(columns))]

                existing_primary_key = self.select(new_primary_key, primary_key_col, column_mask)
                if len(existing_primary_key) == 0:
                    pass
                else:
                    # update failed because primary key already exists
                    # print(f"Skipping DUPLICATE Tail for Base RID::{rid}")
                    return False
            # primary key is not being updated, or it is and wasn't already in the table
            result = self.table.append_tail_record(rid, columns)
            if type(result) == bool:
                return result
            else:
                return False
        except Exception as e:
            print(f"Exception in update :: {e} {traceback.format_exc()}")
            return False


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
    def sum_version(self, start_range:int, end_range:int, aggregate_column_index:int, relative_version:int) -> int:
        if end_range < start_range:
            tmp = end_range
            end_range = start_range
            start_range = tmp
        try:
            # ask index to find the relevant RIDs
            # print("searching for rids", start_range, end_range, aggregate_column_index)
            # using col_num 0 becasue that is the primary key's index
            rid_set = self.table.index.locate_range(start_range, end_range, 0)
            if len(rid_set) == 0:
                return 0
            # get the attribute values and return the sum
            # build a all 0 column mask except for the aggregate column
            column_mask = [0]*self.table.num_columns
            column_mask[aggregate_column_index] = 1
            # print("sum", column_mask, aggregate_column_index)
            sum_value = 0
            # sum records after applying tails
            for rid in rid_set:
                record = self.table.locate_record(rid, 0, column_mask, relative_version)
                sum_value += record.columns[aggregate_column_index]
                # print("sum value", sum_value)
            if type(sum_value) == int:
                return sum_value
            else:
                return 0
        except Exception as e:
            print(f"Exception in sum_version :: {e} {traceback.format_exc()}")
            return 0


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
