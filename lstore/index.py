"""
Index Class


A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
This implementation will use dictionaries.
However, might use B-Tree later for potential performance benefits on range queries.
"""

from typing import NewType, List, Union

# NOTE: Assuming RIDs are integers for typing purposes
RID = NewType('RID', int)


class Index:

    def __init__(self, table: "Table"):
        # One index per column
        self.table: "Table" = table
        self.indices: List[Union[None, dict[int, List[RID]]]] = [None] * table.num_columns

    def locate(self, column_num: int, value: int) -> List[RID]:
        """
        Returns the RIDs of all records with the given value in the specified column
        """
        if self.indices[column_num] is None:
            # index this column
            self.create_index(column_num)
        return self.indices[column_num][value]

    def locate_range(self, start_val: int, end_val: int, column_num: int) -> List[RID]:
        """
        Returns the RIDs of all records with values within specified range in specified column
        """
        result = []
        for val in range(start_val, end_val + 1):
            result.extend(self.locate(column_num, val))
        return result

    def add_record_to_index(self, record: "Record") -> None:
        """
        Add the RID to appropriate existing indices for columns.
        It is assumed that the Record object will have the RID and the values for each column.
        """
        assert self.table.num_columns == len(record.columns)
        for i in range(self.table.num_columns):
            if self.indices[i] is None:
                # column is not being indexed, skip it
                continue
            if self.indices[i].get(record.columns[i]) is None:
                # new entry for this value
                self.indices[i][record.columns[i]] = [record.rid]
            else:
                # other records also have this value, add to the list
                self.indices[i][record.columns[i]].append(record.rid)
        pass

    def remove_record_from_index(self, record: "Record") -> None:
        """
        Remove the RID from all indices.
        This function could be implemented w/ RID instead of Record, but it would take longer to find RID in each index.
        """
        for i in range(len(self.indices)):
            self.indices[i] = None

    def create_index(self, column_num: int) -> None:
        """
        Index the specified column from scratch.
        If an index already exists, it will be overwritten.
        """
        print(f"Indexing column {column_num}...")
        # NOTE this must be updated if the table implementation changes
        for page in self.table.page_directory[column_num]:
            # iterate over all pages of the column
            # NOTE pseudocode below; we must first define bytearray format for pages
            for column_record in page.records:
                # iterate over all records in the page
                self.indices[column_num][column_record.value] = column_record.rid
        pass

    def drop_index(self, column_num: int) -> None:
        """
        Drop the index of the specified column.
        """
        self.indices[column_num] = None
