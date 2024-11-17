"""
Index Class


A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
This implementation will use dictionaries.
However, might use B-Tree later for potential performance benefits on range queries.
"""

from typing import NewType, List, Union
from lstore.bplus_tree import BPlusTree
import lstore.config as config
from lstore.bplus_tree import RID

class Index:

    def __init__(self, table: "Table", use_bplus:bool=config.INDEX_USE_BPLUS_TREE, degree:int=config.INDEX_BPLUS_TREE_MAX_DEGREE):
        # One index per column
        self.table: "Table" = table
        self.indices: Union[List[Union[None, BPlusTree]], List[Union[None, dict[int, List[RID]]]]]
        self.tree_index: bool = use_bplus
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
            self.indices[col_num].update(new_val, curr_val, rid)
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
            raise ValueError("Tried to create an empty index of an already-indexed column.")
        if self.tree_index:
            # create BPlusTree index
            self.indices[column_num] = BPlusTree(max_degree=self.degree)
        else:
            # create dict index
            self.indices[column_num] = {}

    def drop_index(self, column_num: int) -> None:
        """
        Drop the index of the specified column.
        Warning: It will not be possible to recover the index, even by calling create_index.
        """
        self.indices[column_num] = None
