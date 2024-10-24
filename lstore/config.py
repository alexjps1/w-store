from lstore.rid import coords_to_rid
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
MAX_COLUMNS = 2**FIXED_PARTIAL_RECORD_SIZE
SCHEMA_TERMINATION_VALUE = 9

# define RID attribute bit sizes
# This defines the following constraints under the (1, 21, 10) = 32 format
# - Only 4 billion inserts possible
# - Max of 32 columns
#  - Only supports 4 byte integer values
TAIL_BIT = 1
PAGE_NUMBER_BITS = 21
OFFSET_BITS = 10
RID_BIT_SIZE = TAIL_BIT + PAGE_NUMBER_BITS + OFFSET_BITS

RID_TOMBSTONE_VALUE = coords_to_rid(False, 2**21-1, 2**10-1)

RID_COLUMN = 0
INDIRECTION_COLUMN = 1
SCHEMA_ENCODING_COLUMN = 2
TIMESTAMP_COLUMN = 3
NUM_METADATA_COLUMNS = 4  # just the number of metadata columns

def schema_AND(list_one:list[bool]|bytearray|list[int], list_two:list[bool]|bytearray|list[int]) -> list[bool]|list[int]:
    assert len(list_one) == len(list_two)
    # calculates logical AND for each pair of elements in list_one and list_two
    return [a and b for a, b in zip(list_one, list_two)]

def schema_SUBTRACT(column_mask:list[bool]|bytearray|list[int], schema:list[bool]|bytearray|list[int]) -> list[bool]|list[int]:
    assert len(column_mask) == len(schema)
    # subtracts schema from column_mask, True - False = True, True - True = False, False - True = False, False - False = False
    return [a and not b for a, b in zip(column_mask, schema)]
        
def int_to_bytearray(data:int) -> bytearray:
    """
    Stores an integer as a bytearray with length set by FIXED_PARTIAL_RECORD_SIZE
    Inputs: data, integer to store in the bytearray
    Outputs: a bytearray with size equal to FIXED_PARTIAL_RECORD_SIZE
    """
    return bytearray(data.to_bytes(FIXED_PARTIAL_RECORD_SIZE, 'little'))

def bytearray_to_int(array:bytearray) -> int:
    """
    Retrieves an int that was stored in a bytearray
    Inputs: array, the bytearray to convert to an int
    Outputs: an int
    """
    return int.from_bytes(bytes(array), 'little')

def schema_to_bytearray(schema:list[bool]|list[int]) -> bytearray:
    """
    Converts a schema encoding to a bytearray for storage
    Inputs: schema, a list of 1 or 0 values
    Outputs: a bytearray of length equal to FIXED_PARTIAL_RECORD_SIZE containing the schema
    """
    array = bytearray(FIXED_PARTIAL_RECORD_SIZE)
    for i, value in enumerate(schema):
        array[i] = value
    array[len(schema)] = SCHEMA_TERMINATION_VALUE # add a termination character
    return array

def bytearray_to_schema(array:bytearray) -> list[bool]|list[int]:
    """
    Converts a bytearray to the schema encoding data, this function requires the schema be terminated by the SCHEMA_TERMINATION_VALUE
    Inputs: array, the bytearray storing the schema
    Outputs: the schema as a list of 1 or 0 values
    """
    l = [0]*MAX_COLUMNS
    trim_index = MAX_COLUMNS
    for i, value in enumerate(array):
        l[i] = value # value is already an int
        if l[i] == SCHEMA_TERMINATION_VALUE:
            trim_index = i
            break
    return l[:trim_index-1]
