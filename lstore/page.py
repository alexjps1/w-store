"""
Module Interfaces:
    Config for page size
"""
from lstore.config import PAGE_SIZE, FIXED_PARTIAL_RECORD_SIZE


class Page:
    """
    Page represents one column in the database. one-column-partial-records in the page are stored in a bytearray of length set by the PAGE_SIZE constant in the Config class.
    The Page class can:
        - Check if a new record can be added
        - Write a new record
    """
    def __init__(self, page_size=PAGE_SIZE, record_size=FIXED_PARTIAL_RECORD_SIZE) -> None:
        # num_records is a count of how many records are contained in this page (column)
        self.page_size:int = page_size
        self.record_size: int = record_size
        self.num_records:int = 0
        self.data:bytearray = bytearray(self.page_size)

    def has_capacity(self) -> bool:
        """
        Function checks if this page has enough space left for a new record (1 column of a record) to be added.

        Inputs: None for fixed size records
        Outputs: True if the page has enough space for the new record, otherwise False
        """
        return self.record_size * self.num_records <self.page_size 

    def write_direct(self, value:bytearray) -> None:
        """
        Writes a new bytearray to the page, and increments num_records by one.
        Inputs: value that will be written to the page
        Outputs: None
        """
        # this is the location of the start of this page entry
        offset = self.record_size * self.num_records
        # set the data at the calculated offset
        self.data[offset:offset + self.record_size] = value
        # we have +1 records in this column
        self.num_records += 1

    def overwrite_direct(self, value:bytearray, offset:int) -> None:
        """
        Overwrites the data at the given offset with the new value, this function should only be called to overwrite the indirection column of a base record to update it with the new current tail record.
        """
        overwrite_offset = self.record_size * offset
        # set the data at the calculated offset
        self.data[overwrite_offset:overwrite_offset + self.record_size] = value
        

    def retrieve_direct(self, offset:int) -> bytearray:
        """
        Retrieves the partial record located at the given offset
        Inputs: offset, the record number for this page
        Outputs: the bytearray representing the record
        """
        byte_offset = offset *self.record_size 
        return self.data[byte_offset:byte_offset + self.record_size]
