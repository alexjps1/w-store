
"""
Centralized storage for all configuration options and constants.
Imported by other modules when they need access to a configuration option or a constant.

Inputs: None
Outputs: None
Module Interfaces: None
"""
# PAGE_SIZE is the byte capacity for each page
PAGE_SIZE:int = 4096
# FIXED_PAGE_DATA_SIZE is the length in bytes of the data stored in each page (1 entry, for 1 column)
FIXED_PARTIAL_RECORD_SIZE:int = 4 # the size of data allowed in each column, used to calculate offsets within pages
MAX_COLUMNS = 32

# define RID attribute bit sizes
# This defines the following constraints under the (1, 21, 10) = 32 format
# - Only 4 billion inserts possible
# - Max of 32 columns
#  - Only supports 4 byte integer values
TAIL_BIT = 1
PAGE_NUMBER_BITS = 21
OFFSET_BITS = 10
RID_BIT_SIZE = TAIL_BIT + PAGE_NUMBER_BITS + OFFSET_BITS

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
TIMESTAMP_COLUMN = 3
NUM_METADATA_COLUMNS = 4  # just the number of metadata columns

def schema_AND(list_one:list[bool], list_two:list[bool]) -> list[bool]:
    assert len(list_one) == len(list_two)
    # calculates logical AND for each pair of elements in list_one and list_two
    return [a and b for a, b in zip(list_one, list_two)]

def schema_SUBTRACT(column_mask:list[bool], schema:list[bool]) -> list[bool]:
    assert len(column_mask) == len(schema)
    # subtracts schema from column_mask, True - False = True, True - True = False, False - True = False, False - False = False
    return [a and not b for a, b in zip(column_mask, schema)]
        

