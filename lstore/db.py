from pathlib import Path
from lstore.table import Table
from lstore.config import DATABASE_DIR, NUM_METADATA_COLUMNS, FIXED_PARTIAL_RECORD_SIZE, RID_COLUMN, OVERRIDE_WITH_DUMB_INDEX
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
        self.tables:dict[str, Table] = {}   # this assumes no 2 tables have the same name
        self.database_path:Path = Path("default")
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
            # load all tables in database
            # print(f"loading database {path}")
            for file in full_path.iterdir():
                if file.is_dir():
                    # print(f"loading table {file.stem}")
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
        else:
            # table not loaded, but files may be present
            dir_name = Path(DATABASE_DIR, self.database_path, name)
            if dir_name.exists():
                for file in dir_name.iterdir():
                    #Remove file if it is a file
                    if file.is_file():
                        file.unlink()
                #Delete the table
                dir_name.rmdir()

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name:str) -> Table|Literal[False]:
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
            if not OVERRIDE_WITH_DUMB_INDEX: self.generate_index_on_loaded_table(path, table)
            # update table dictionary
            self.tables[name] = table
            # return the table
            return table
        else:
            return False

    def generate_index_on_loaded_table(self, path:Path, table:Table) -> None:
        """
        Generates a new index by reading saved page files. Used to generate a new index when a table is loaded, but not when a new table is made.
        """
        all_base_pages = sorted(path.glob("b_col*_*.bin"))
        last_values = {}
        # generate an index on all base records in the data columns
        for page in all_base_pages:
            # generate an index on all data columns
            _, column_str, page_num = str(page.stem).split('_')
            column_num = int(column_str[3:])
            # ignore all metadata columns
            if column_num >= NUM_METADATA_COLUMNS:
                # is a data column, generate an index
                # read data page
                page_data = bytearray(page.read_bytes())
                num_records = bytearray_to_int(page_data[:FIXED_PARTIAL_RECORD_SIZE])
                data = page_data[FIXED_PARTIAL_RECORD_SIZE:]
                # read rid page
                rid_page = Path(path, f"b_col{RID_COLUMN}_{page_num}.bin")
                rid_page_data = bytearray(rid_page.read_bytes())
                rid_data = rid_page_data[FIXED_PARTIAL_RECORD_SIZE:]
                # build an index entry for all records in page
                for offset in range(num_records):
                    byte_offset = offset * FIXED_PARTIAL_RECORD_SIZE
                    # get rid
                    rid = bytearray_to_int(rid_data[byte_offset:byte_offset + FIXED_PARTIAL_RECORD_SIZE])
                    # get column value
                    value = bytearray_to_int(data[byte_offset:byte_offset + FIXED_PARTIAL_RECORD_SIZE])
                    # save for tail record indexes
                    last_values[rid] = value
                    # add index entry
                    table.index.add_record_to_index(column_num, value, rid)

        all_tail_pages = sorted(path.glob("t_col*_*.bin"))
        # generate an index on all tail records in the data columns
        for page in all_tail_pages:
            # generate an index on all data columns
            _, column_str, page_num = str(page.stem).split('_')
            column_num = int(column_str[3:])
            # ignore all metadata columns
            if column_num >= NUM_METADATA_COLUMNS:
                # is a data column, generate an index
                # read data page
                page_data = bytearray(page.read_bytes())
                num_records = bytearray_to_int(page_data[:FIXED_PARTIAL_RECORD_SIZE])
                data = page_data[FIXED_PARTIAL_RECORD_SIZE:]
                # read rid page
                base_rid_page = Path(path, f"b_col{RID_COLUMN}_{page_num}.bin")
                base_rid_page_data = bytearray(base_rid_page.read_bytes())
                base_rid_data = base_rid_page_data[FIXED_PARTIAL_RECORD_SIZE:]
                # build an index entry for all records in page
                for offset in range(num_records):
                    byte_offset = offset * FIXED_PARTIAL_RECORD_SIZE
                    # get rid
                    base_rid = bytearray_to_int(base_rid_data[byte_offset:byte_offset + FIXED_PARTIAL_RECORD_SIZE])
                    # get column value
                    value = bytearray_to_int(data[byte_offset:byte_offset + FIXED_PARTIAL_RECORD_SIZE])
                    # add index entry
                    table.index.update_record_in_index(column_num, last_values[base_rid], base_rid, value)
