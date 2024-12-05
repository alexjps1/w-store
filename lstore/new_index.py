"""
New Index Class


A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
This implementation will use dictionaries.
However, might use B-Tree later for potential performance benefits on range queries.

#NOTE: select_version is handled by the table and not the index, for past versions (MAJOR CHANGE!!!)
"""

from typing import NewType, List, Union
from lstore.bplus_tree import BPlusTree
from lstore.hashtable_index import HashtableIndex
import lstore.config as config
from lstore.config import debug_print as print
from lstore.bplus_tree import RID

class New_Index:

    def __init__(self, table: "Table", use_bplus:bool=config.INDEX_USE_BPLUS_TREE, use_hash:bool=config.INDEX_USE_HASH, degree:int=config.INDEX_BPLUS_TREE_MAX_DEGREE):
        # One index per column
        self.table: "Table" = table
        self.indices: Union[List[Union[None, HashtableIndex]], List[Union[None, dict[int, List[RID]]]]]
        self.tree_index: bool = use_bplus
        self.hash_index: bool = use_hash
        self.degree: int = degree
        self.indices = [None] * table.num_columns
        if config.INDEX_AUTOCREATE_ALL_COLS:
            # create an index for all columns
            for i in range(table.num_columns):
                self.create_index(i)

    def locate(self, column_num: int, value: int) -> List[RID]:
        """
        Returns the RIDs of all records with the given value in the specified column
        """
        if self.indices[column_num] is None:
            if config.INDEX_USE_DUMB_INDEX:
                raise NotImplementedError("The desired column is not indexed and using dumb index to locate is not yet implemented.")
            else:
                raise ValueError("The desired column is not indexed and the configuration does not allow using dumb index to locate records.")
        if self.tree_index:
            # run a point query on the tree
            return self.indices[column_num].point_query(value)
        elif self.hash_index:
            return self.indices[column_num].point_query(value)
        # run a point query on the dictionary
        return self.indices[column_num][value]

    def locate_range(self, start_val: int, end_val: int, col_num: int) -> List[RID]:
        """
        Returns the RIDs of all records with values within specified range in specified column
        """
        if self.indices[col_num] is None:
            # this column is not indexed
            if config.INDEX_USE_DUMB_INDEX:
                raise NotImplementedError("The desired column is not indexed and using dumb index to locate is not yet implemented.")
            else:
                raise ValueError("The desired column is not indexed and the configuration does not allow using dumb index to locate records.")
        result = []
        if self.tree_index:
            # run a range query on the B+ tree
            return self.indices[col_num].range_query(start_val, end_val)
        elif self.hash_index:
            return self.indices[col_num].range_query(start_val, end_val)
        # iterate through all values in the range and grab from dict
        for val in range(start_val, end_val + 1):
            extension = self.indices[col_num].get(val)
            result.extend(extension if extension is not None else [])
        return result

    def locate_version(self, col_num: int, value: int, rel_ver: int):
        """
        Returns the RIDs of all records with the given value in the specified column and version
        """
        if self.indices[col_num] is None:
            if config.INDEX_USE_DUMB_INDEX:
                raise NotImplementedError("The desired column is not indexed and using dumb index to locate is not yet implemented.")
            else:
                raise ValueError("The desired column is not indexed and the configuration does not allow using dumb index to locate records.")
        result = []
        if self.tree_index:
            # run a point query on the tree
            return self.indices[col_num].version_query(value, rel_ver)
        elif self.hash_index:
            if rel_ver==0:
                return self.indices[col_num].version_query(value, rel_ver)
            else:
                return self.table.dumb_index.locate_version(col_num, value, rel_ver)
        raise NotImplementedError("Tried to locate version in dict index, not compatible with versioning at this time.")


    def add_record_to_index(self, col_num: int, val: int, rid: RID) -> None:
        """
        Add the RID to the appropriate index for the specified column and value.
        This should be called once for every column on record insertion.
        Calls to this function on unindexed columns will be ignored.
        """
        if self.indices[col_num] is None:
            return
        if self.tree_index:
            self.indices[col_num].insert(val, rid)
            return
        elif self.hash_index:
            self.indices[col_num].insert(val, rid)
            return
        raise NotImplementedError("Tried to insert to dict index, not compatible with versioning at this time.")
        assert isinstance(self.indices[col_num], dict)
        if self.indices[col_num].get(val) is None:
            # new entry for this value
            self.indices[col_num][val] = [rid]
        else:
            # other records also have this value, add to the list
            self.indices[col_num][val].append(rid)

    def update_record_in_index(self, col_num: int, curr_val: int, rid: RID, new_val: int):
        """
        Add a new entry to the index which references the previous value of the record.
        """
        if self.tree_index:
            assert isinstance(self.indices[col_num], BPlusTree)
            self.indices[col_num].update(new_val, rid)
            return
        elif self.hash_index:
            assert isinstance(self.indices[col_num], HashtableIndex)
            self.indices[col_num].update(new_val, rid)
            return
        raise NotImplementedError("Tried to update dict index, not compatible with versioning at this time.")

    def remove_record_from_index(self, col_num: int, val: int, rid: RID) -> None:
        """
        Remove an entry from an index at a particular column.
        This should be called once for every column on record deletion.
        Calls to this function on unindexed columns will be ignored.
        """
        if self.indices[col_num] is None:
            return
        if self.tree_index:
            if self.indices[col_num].delete(val, rid) is False:
                raise ValueError("The key to delete is not in the index.")
            return
        if self.hash_index:
            if self.indices[col_num].delete(val, rid) is False:
                # print(f"Delete failed at column number: {col_num}")
                raise ValueError("The key to delete is not in the index.")
            return
        raise NotImplementedError("Tried to delete from dict index, not compatible with versioning at this time.")
        if self.indices[col_num].get(val) is None:
            raise ValueError("The key to delete is not in the index.")
        self.indices[col_num][val].remove(rid)


    def create_index(self, column_num: int) -> None:
        """
        Create an empty index for the specified column.
        Warning: Run only on an empty table. Otherwise, the index will be incomplete.
        """
        if self.indices[column_num] is not None:
            return None
        if self.tree_index:
            # create BPlusTree index
            self.indices[column_num] = BPlusTree(max_degree=self.degree)
        elif self.hash_index:
            self.indices[column_num] = HashtableIndex()
        else:
            # create dict index
            self.indices[column_num] = {}

    def load_index_from_disk(self, path:str):
        """Path is the file path up to the table name"""
        if self.hash_index:
            col_num = 1
            for index in self.indices:
                index.load_index(path, col_num)
                col_num += 1
        else:
            raise NotImplementedError("This function is called only for hastable indices")

    def save_index_to_disk(self, path:str):
        """Path is the file path up to the table name"""
        if self.hash_index:
            col_num = 1
            for index in self.indices:
                index.save_index(path, col_num)
                col_num += 1
        else:
            raise NotImplementedError("This function is called only for hashtable indices")

    def drop_index(self, column_num: int) -> None:
        """
        Drop the index of the specified column.
        Warning: It will not be possible to recover the index, even by calling create_index.
        """
        self.indices[column_num] = None
