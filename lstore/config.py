from math import log
from pathlib import Path
"""
Centralized storage for all configuration options and constants.
Imported by other modules when they need access to a configuration option or a constant.

Inputs: None
Outputs: None
Module Interfaces: None
"""
"""
RID Conversion Tool

Tool to convert between RIDs and coordinates identifying the location of a record.
"""

def coords_to_rid(is_tail: bool, page_num: int, offset: int) -> int:
    """
    Converts the page number and offset to a RID
    """
    tail_bit = 0b1 if is_tail else 0b0
    tail_component = tail_bit << (PAGE_NUMBER_BITS + OFFSET_BITS)

    page_mask = (page_num << OFFSET_BITS) - 1
    page_component = (page_mask & page_num) << OFFSET_BITS

    offset_mask = (offset << OFFSET_BITS) - 1
    offset_component = offset_mask & offset

    return tail_component | page_component | offset_component


def rid_to_coords(rid: int) -> tuple[bool, int, int]:
    """
    Converts the RID to page number and offset
    """
    tail_bit = (rid >> (PAGE_NUMBER_BITS + OFFSET_BITS)) & 0b1
    page_num = (rid >> OFFSET_BITS) & ((1 << PAGE_NUMBER_BITS) - 1)
    offset = rid & ((1 << OFFSET_BITS) - 1)

    return bool(tail_bit), page_num, offset

"""
Constants
"""
# PAGE_SIZE is the byte capacity for each page
PAGE_SIZE:int = 4096
# FIXED_PAGE_DATA_SIZE is the length in bytes of the data stored in each page (1 entry, for 1 column)
FIXED_PARTIAL_RECORD_SIZE:int = 8 # the size of data allowed in each column, used to calculate offsets within pages
BUFFERPOOL_SIZE = 15 # number of pages allowed in the bufferpool
DATABASE_DIR = Path("disk")
# MAX_COLUMNS = 0 # total number of data + metadata columns addressable for a table
CUMULATIVE_TAIL_RECORDS = True

# Index options
OVERRIDE_WITH_DUMB_INDEX: bool = True
INDEX_USE_BPLUS_TREE: bool = True  # if False, use dictionary-based index (bad for range queries)
INDEX_AUTOCREATE_ALL_COLS: bool = True  # if False, columns must be explicitly indexed before use
INDEX_USE_DUMB_INDEX: bool = True  # if True, use dumb index to find records on unindexed col; False, throw error
INDEX_BPLUS_TREE_MAX_DEGREE: int = 4  # max degree of B+ tree nodes


# define RID attribute bit sizes
# This defines the following constraints under the (1, 21, 10) = 32 format
# - Only 4 billion inserts possible
# - Max of 32 columns
#  - Only supports 4 byte integer values
# update to (1, 54, 9) = 64 format
FORMAT_BITS = 8 * FIXED_PARTIAL_RECORD_SIZE # currently 64
TAIL_BIT = 1
OFFSET_BITS = int(log(PAGE_SIZE / FIXED_PARTIAL_RECORD_SIZE, 2)) # 2**OFFSET_BITS = PAGE_SIZE / FIXED_PARTIAL_RECORD_SIZE
# OFFSET_BITS = 9 # 2**OFFSET_BITS = PAGE_SIZE / FIXED_PARTIAL_RECORD_SIZE
PAGE_NUMBER_BITS = FORMAT_BITS - TAIL_BIT - OFFSET_BITS # PAGE_NUMBER_BITS = 54
RID_BIT_SIZE = TAIL_BIT + PAGE_NUMBER_BITS + OFFSET_BITS

RID_TOMBSTONE_VALUE = coords_to_rid(False, 2**PAGE_NUMBER_BITS-1, 2**OFFSET_BITS-1)

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
# TIMESTAMP_COLUMN= 3
CREATED_TIME_COLUMN = 3
UPDATED_TIME_COLUMN = 4
NUM_METADATA_COLUMNS = 5  # just the number of metadata columns

# number of update transactions until merge is called (on filled head pages)
NUM_UPDATES_TO_MERGE = 100

def schema_AND(list_one:list[int], list_two:list[int]) -> list[int]:
    assert len(list_one) == len(list_two)
    # calculates logical AND for each pair of elements in list_one and list_two
    return [int(a and b) for a, b in zip(list_one, list_two)]

def schema_SUBTRACT(column_mask:list[int], schema:list[int]) -> list[int]:
    assert len(column_mask) == len(schema)
    # subtracts schema from column_mask, True - False = True, True - True = False, False - True = False, False - False = False
    return [int(a and not b) for a, b in zip(column_mask, schema)]

def int_to_bytearray(data:int, record_size:int=FIXED_PARTIAL_RECORD_SIZE) -> bytearray:
    """
    Stores an integer as a bytearray with length set by FIXED_PARTIAL_RECORD_SIZE
    Inputs: data, integer to store in the bytearray
    Outputs: a bytearray with size equal to FIXED_PARTIAL_RECORD_SIZE
    """
    return bytearray(data.to_bytes(record_size, 'little'))

def bytearray_to_int(array:bytearray) -> int:
    """
    Retrieves an int that was stored in a bytearray
    Inputs: array, the bytearray to convert to an int
    Outputs: an int
    """
    return int.from_bytes(bytes(array), 'little')

def schema_to_bytearray(schema:list[bool]|list[int], record_size:int=FIXED_PARTIAL_RECORD_SIZE) -> bytearray:
    """
    Converts a schema encoding to a bytearray for storage, by first converting the schema to an int, and then a bytearray
    Inputs: schema, a list of 1 or 0 values
    Outputs: a bytearray of length equal to FIXED_PARTIAL_RECORD_SIZE containing the schema
    """
    num = int(''.join(str(x) for x in schema), 2)
    return int_to_bytearray(num, record_size)

def bytearray_to_schema(array:bytearray, length:int) -> list[bool]|list[int]:
    """
    Converts a bytearray to the schema encoding data, by converting the bytearray to an int and then to the schema

    Inputs: array, the bytearray storing the schema
    Outputs: the schema as a list of 1 or 0 values
    """
    num = bytearray_to_int(array)
    return [int(x) for x in list('{0:0b}'.format(num).zfill(length))]
