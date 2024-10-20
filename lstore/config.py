
class Config:
    """
    Centralized storage for all configuration options and constants.
    Imported by other modules when they need access to a configuration option or a constant.

    Inputs: None
    Outputs: None
    Module Interfaces: None
    """
    def __init__(self) -> None:
        # PAGE_SIZE is the byte capacity for each page
        self.PAGE_SIZE:int = 4096
        # FIXED_PAGE_DATA_SIZE is the length in bytes of the data stored in each page (1 entry, for 1 column)
        self.FIXED_PAGE_DATA_SIZE:int = 32 # TODO replace with actual value

        # define RID attribute bit sizes
        # This defines the following constraints under the (1, 21, 10) = 32 format
        # - Only 4 billion inserts possible
        # - Max of 32 columns
        #  - Only supports 4 byte integer values
        self.TAIL_BIT = 1
        self.PAGE_NUMBER_BITS = 21
        self.OFFSET_BITS = 10
        self.RID_BIT_SIZE = self.TAIL_BIT + self.PAGE_NUMBER_BITS + self.OFFSET_BITS
