from pathlib import Path
from lstore.table import Table
from lstore.config import DATABASE_DIR, NUM_METADATA_COLUMNS, FIXED_PARTIAL_RECORD_SIZE, RID_COLUMN, \
    OVERRIDE_WITH_DUMB_INDEX
from lstore.config import bytearray_to_int
from lstore.config import debug_print as print
from typing import Literal


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
        self.tables: dict[str, Table] = {}  # this assumes no 2 tables have the same name
        self.database_path: Path = Path("default")
        self.table_info_file = "__table_info__.bin"

    def open(self, path: str):
        """
        Loads the database from disk.
        Inputs: path, the location of the database.
        Outputs: None, self.tables is updated by self.get_table
        """
        full_path = Path(DATABASE_DIR, path)
        self.database_path = Path(path)
        # print(full_path, path, self.database_path)
        if full_path.exists():
            # load all tables in database
            # print(f"loading database {path}")
            for file in full_path.iterdir():
                if file.is_dir():
                    # print(f"loading table {file.stem}")
                    # load the table (along with its index) from disk
                    self.get_table(str(file.stem))
                # print(f"finished loading table {file.stem}")
            # print(f"finished loading database {path}")
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
            table.index.save_index_to_disk(Path(DATABASE_DIR, self.database_path, table.name))

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name: str, num_columns: int, key_index: int, **kwargs) -> Table:
        """
        Creates a new table and returns it.

        Inputs:
            - name, the name of the new table.
            - num_columns, the number of columns in the table.
            - key_index, TODO
        Outputs: table, the new table.
        """
        # make a new table
        path = Path(DATABASE_DIR, f"{self.database_path}", f"{name}")
        if path.exists():
            # existing table, clear data
            print(f"clearing existing table {name}")
            self.drop_table(name)
        # dir is either not there, or we just deleted it
        # print(f"making new table {name}")
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

    def drop_table(self, name: str) -> None:
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
        else:
            # table not loaded, but files may be present
            dir_name = Path(DATABASE_DIR, self.database_path, name)
            if dir_name.exists():
                for file in dir_name.iterdir():
                    # Remove file if it is a file
                    if file.is_file():
                        file.unlink()
                # Delete the table
                dir_name.rmdir()

    """
    # Returns table with the passed name
    """

    def get_table(self, name: str) -> Table | Literal[False]:
        """
        Finds and returns the table with the given name.

        Inputs: name, the name of the table to return.
        Outputs: Table object with the given name OR False if table could not be found
        """
        # if table is already loaded, return it
        if name in self.tables.keys():
            print(f"table {name} already loaded")
            return self.tables[name]
        # load table from disk
        path = Path(DATABASE_DIR, f"{self.database_path}", f"{name}")
        # table info file path
        table_info_file = Path(path, self.table_info_file)
        if path.exists():
            # get the table info
            num_col, key_index = list(table_info_file.read_bytes())
            # build table object
            table = Table(name, self.database_path, num_col, key_index)
            # regenerate the index
            # if not OVERRIDE_WITH_DUMB_INDEX: self.generate_index_on_loaded_table(path, table)
            # load the index from disk
            table.index.load_index_from_disk(path)
            # update table dictionary
            self.tables[name] = table
            # return the table
            return table
        else:
            return False
