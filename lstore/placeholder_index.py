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
            

    def locate_range(self, start_val:int, end_val:int, column_num:int) -> list[int]|Literal[False]:
        rids:list[int] = []
        found_end = False
        found_start = False
        # check all pages for start_val
        for n, page in enumerate(self.table.page_directory[column_num + NUM_METADATA_COLUMNS]["base"]):
            # check all offsets
            for i in range(page.num_records):
                if bytearray_to_int(page.retrieve_direct(i)) == start_val:
                    found_start = True
                if bytearray_to_int(page.retrieve_direct(i)) == end_val:
                    found_end = True
                    # assuming inclusive range
                    rids.append(self.get_rid(n, i))

                if found_start and not found_end:
                    # add rid while in the range
                    rids.append(self.get_rid(n, i))
                
        # return values only if both start and end were found
        if found_start and found_end:
            return rids
        else:
            return False
