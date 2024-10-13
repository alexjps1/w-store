"""
Module Interfaces:
    Config for page size
    Record for writing records to the page
"""
from lstore.config import Config
config = Config()

from lstore.table import Record

class Page:
    """
    Page represents one column in the database. Records in the page are stored in a bytearray of length set by the PAGE_SIZE constant in the Config class.
    The Page class can:
        - Check if a new record can be added
        - Write a new record
    """
    def __init__(self) -> None:
        # num_records is a count of how many records are contained in this page (column)
        self.num_records:int = 0
        self.data:bytearray = bytearray(config.PAGE_SIZE)

    def has_capacity(self) -> bool:
        """
        Function checks if this page has enough space left for a new record to be added.

        Inputs: None for fixed size records || new_record_size:int for variable sized records
        Outputs: True if the page has enough space for the new record, otherwise False
        """
        pass

    def write(self, value:Record) -> None:
        """
        Writes a new record to the page, and increments num_records by one.
        Inputs: value the new record that will be written to the page
        Outputs: None
        """
        self.num_records += 1
        pass

