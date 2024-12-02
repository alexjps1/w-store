from lstore.config import RID_COLUMN, NUM_METADATA_COLUMNS
from lstore.config import bytearray_to_int
from lstore.config import debug_print as print

class DumbIndex:
    """
    Dumb index class to allow testing, it is really inefficient
    """
    def __init__(self, table:"Table") -> None:
        self.table = table

    def get_rid(self, page_num:int, offset:int) -> int:
        page = self.table.page_directory.retrieve_page(RID_COLUMN, False, page_num)
        if page is not None:
            return bytearray_to_int(page.retrieve_direct(offset))
        else:
            assert False

    def locate(self, column_num:int, value:int) -> list[int]:
        rids:list[int] = []
        # print(f"-> locate::{value}, in::{column_num}, for page range 0,{self.table.current_base_page_number}")
        # check all pages
        for n in range(self.table.current_base_page_number + 1):
            page = self.table.page_directory.retrieve_page(column_num + NUM_METADATA_COLUMNS, False, n)
            if page is not None:
                # check all offsets
                for i in range(page.num_records):
                    if bytearray_to_int(page.retrieve_direct(i)) == value:
                        # add rid to locate list
                        rids.append(self.get_rid(n, i))
        return rids
            
    def locate_version(self, col_num:int, value:int, rel_ver:int):
        return self.locate(col_num, value)

    def locate_range(self, start_key:int, end_key:int, col_num: int) -> list[int]:
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
        for n in range(self.table.current_base_page_number + 1):
            page = self.table.page_directory.retrieve_page(0 + NUM_METADATA_COLUMNS, False, n)
            if page is not None:
                # check all offsets
                for i in range(page.num_records):
                    # if the value is in range add it
                    if bytearray_to_int(page.retrieve_direct(i)) >= start_key and bytearray_to_int(page.retrieve_direct(i)) <= end_key:
                        # assuming inclusive range
                        rids.append(self.get_rid(n, i))
        # return values only if both start and end were found
        return sorted(rids)

    def add_record_to_index(self, *args) -> None:
        pass

    def remove_record_from_index(self, *args) -> None:
        pass

    def create_index(self, *args) -> None:
        pass

    def drop_index(self, *args) -> None:
        pass

    def update_record_in_index(self, *args) -> None:
        pass
