
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
        self.FIXED_PAGE_DATA_SIZE:int = 16 # TODO replace with actual value
