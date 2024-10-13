"""
Index Class

A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
This implementation will use dictionaries.
However, might use B-Tree later for potential performance benefits on range queries.
"""

from typing import NewType, List

# NOTE: Assuming RIDs are integers for typing purposes
RID = NewType('RID', int)

class Index:

    def __init__(self, table: "Table"):
        # One index per column
        self.indices = [None] * table.num_columns
        pass

    def locate(self, column_num: int, value: int) -> List[RID]:
        """
        Returns the location of all records with the given value in the specified column
        """
        pass

    def locate_range(self, start_val: int, end_val: int, column_num: int) -> List[RID]:
        """
        Returns the RIDs of all records with values within specified range in specified column
        """
        pass

    def add_record_to_index(self, record: "Record") -> None:
        """
        Add the RID to appropriate existing indices for columns.
        It is assumed that the Record object will have the RID and the values for each column.
        """
        pass

    def remove_record_from_index(self, record: "Record") -> None:
        """
        Remove the RID from all indices.
        This function could be implemented w/ RID instead of Record, but it would take longer to find RID in each index.
        """
        pass

    def create_index(self, column_num: int) -> None:
        """
        Index the specified column from scratch.
        If an index already exists, it will be overwritten.
        """
        pass

    def drop_index(self, column_num: int) -> None:
        """
        Drop the index of the specified column.
        """
        pass
