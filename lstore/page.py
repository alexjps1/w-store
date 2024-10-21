"""
Module Interfaces:
    Config for page size
"""
# import config
from lstore.config import PAGE_SIZE, FIXED_PARTIAL_RECORD_SIZE


class Page:
    """
    Page represents one column in the database. one-column-partal-records in the page are stored in a bytearray of length set by the PAGE_SIZE constant in the Config class.
    The Page class can:
        - Check if a new record can be added
        - Write a new record
    """
    def __init__(self) -> None:
        # num_records is a count of how many records are contained in this page (column)
        self.num_records:int = 0
        self.data:bytearray = bytearray(PAGE_SIZE)

    def has_capacity(self) -> bool:
        """
        Function checks if this page has enough space left for a new record (1 column of a record) to be added.

        Inputs: None for fixed size records
        Outputs: True if the page has enough space for the new record, otherwise False
        """
        return FIXED_PARTIAL_RECORD_SIZE * self.num_records <= PAGE_SIZE

    def write(self, value:bytearray) -> None:
        """
        Writes a new bytearray to the page, and increments num_records by one.
        Inputs: value that will be written to the page
        Outputs: None
        """
        # this is the location of the start of this page entry
        offset = FIXED_PARTIAL_RECORD_SIZE * self.num_records
        # set the data at the calculated offset
        self.data[offset:offset + FIXED_PARTIAL_RECORD_SIZE] = value
        # we have +1 records in this column
        self.num_records += 1
