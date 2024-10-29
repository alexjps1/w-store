"""
Module Interfaces:
    Table for building tables
"""
from pathlib import Path
from lstore.table import Table

class Database():
    """
    Handles high-level operations such as:
        - Starting
        - Shutting down
        - Loading the database
        - Creation and deletion of tables (create and drop functions)
    """
    def __init__(self) -> None:
        # set of tables, identified by their name
        self.tables:dict[str, Table] = {}   # this assumes no 2 tables have the same name

    # Not required for milestone1
    def open(self, path:str):
        """
        Loads the database from disk.
        Inputs: path, the location of the database.
        Outputs: the set of tables (self.tables stores this in memory) || None and self.tables is updated within the function.
        """
        python_path = Path(path)
        pass

    # Not required for milestone1 according to available tester code
    def close(self) -> None:
        """
        Shuts down the database, and probably saves the database to disk.
        """
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name:str, num_columns:int, key_index:int, **kwargs) -> Table:
        """
        Creates a new table and returns it.

        Inputs:
            - name, the name of the new table.
            - num_columns, the number of columns in the table.
            - key_index, TODO
        Outputs: table, the new table.
        """
        table = Table(name, num_columns, key_index, **kwargs)
        self.tables[name] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name:str) -> None:
        """
        Deletes the table with the given name.

        Inputs: name, the name of the table to drop
        Outputs: None
        """
        if name in self.tables.keys():
            del self.tables[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name:str) -> Table|bool:
        """
        Finds and returns the table with the given name.

        Inputs: name, the name of the table to return.
        Outputs: Table object with the given name OR False if table could not be found
        """
        if name in self.tables.keys():
            return self.tables[name]
        else:
            return False
