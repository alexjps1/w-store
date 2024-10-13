
class Config:
    """
    Centralized storage for all configuration options and constants.
    Imported by other modules when they need access to a configuration option or a constant.

    Inputs: None
    Outputs: None
    Module Interfaces: None
    """
    def __init__(self) -> None:
        self.PAGE_SIZE:int = 4096
