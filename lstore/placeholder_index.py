from lstore.config import RID_COLUMN, NUM_METADATA_COLUMNS
from lstore.config import bytearray_to_int
# from lstore.table import Table
from typing import Literal

class DumbIndex:
    """
    Dumb index class to allow testing, it is really inefficient
    """
    def __init__(self, table:"Table") -> None:
        self.table = table

    def get_rid(self, page_num:int, offset:int) -> int:
        return bytearray_to_int(self.table.page_directory[RID_COLUMN]["base"][page_num].retrieve_direct(offset))

    def locate(self, column_num:int, value:int) -> list[int]:
        rids:list[int] = []
        # check all pages
        for n, page in enumerate(self.table.page_directory[column_num + NUM_METADATA_COLUMNS]["base"]):
            # check all offsets
            for i in range(page.num_records):
                if bytearray_to_int(page.retrieve_direct(i)) == value:
                    # add rid to locate list
                    rids.append(self.get_rid(n, i))
        return rids
            

    def locate_range(self, start_key:int, end_key:int) -> list[int]:
        """
        Finds the range of RIDs for the given start and end primary key

        Inputs:
            - start_key, the start primary key value of the desired range
            - end_key, the end primary key value of the desired range
        Outputs:
            - a list of rids for the range
        """
        rids:list[int] = []
        # check all pages for start_val, values may not be sorted
        for n, page in enumerate(self.table.page_directory[0 + NUM_METADATA_COLUMNS]["base"]):
            # check all offsets
            for i in range(page.num_records):
                # if the value is in range add it
                if bytearray_to_int(page.retrieve_direct(i)) >= start_key and bytearray_to_int(page.retrieve_direct(i)) <= end_key:
                    # assuming inclusive range
                    rids.append(self.get_rid(n, i))
        # return values only if both start and end were found
        return sorted(rids)
