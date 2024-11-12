from pathlib import Path
from lstore.table import Table
from lstore.config import DATABASE_DIR

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
        self.database_path:Path = DATABASE_DIR
        self.table_info_file = "__table_info__.bin"

    def open(self, path:str):
        """
        Loads the database from disk.
        Inputs: path, the location of the database.
        Outputs: None, self.tables is updated by self.get_table
        """
        full_path = Path(DATABASE_DIR, path)
        self.database_path = Path(path)
        # print(full_path, path, self.database_path)
        if full_path.exists():
            # database already exists, but we will load the tables later
            pass
        else:
            # make a new database
            full_path.mkdir(parents=True)

    def close(self) -> None:
        """
        Shuts down the database, and saves the database to disk.
        """
        # save database to disk
        for table in self.tables.values():
            table.page_directory.save_all()

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
        path = Path(DATABASE_DIR, f"{self.database_path}", f"{name}")
        if not path.exists():
            path.mkdir(parents=True)
        # write table info
        table_info_file = Path(path, self.table_info_file)
        table_info_file.write_bytes(bytearray([num_columns, key_index]))

        # build the table
        table = Table(name, self.database_path, num_columns, key_index, **kwargs)
        # add table to table dictionary
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
            # remove table from disk
            self.tables[name].page_directory.file_manager.delete_files()
            # remove table from database
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
        # load table from disk
        path = Path(DATABASE_DIR, f"{self.database_path}", f"{name}")
        # table info file path
        table_info_file = Path(path, self.table_info_file)
        if path.exists():
            # get the table info
            num_col, key_index = list(table_info_file.read_bytes())
            # build table object
            table = Table(name, self.database_path, num_col, key_index)
            # update table dictionary
            self.tables[name] = table
            # return the table
            return table
        else:
            return False
