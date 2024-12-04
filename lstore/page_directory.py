from lstore.page import Page
from lstore.config import BUFFERPOOL_SIZE, DATABASE_DIR, FIXED_PARTIAL_RECORD_SIZE
from lstore.config import int_to_bytearray, bytearray_to_int
from lstore.config import debug_print as print
from pathlib import Path
from time import time_ns
from shutil import rmtree
"""
Abstraction of the Page Directory and contained Bufferpool
"""

class PageWrapper:
    """
    Wrapper for page objects in the bufferpool, stores page location info as well as last accessed time
    """
    def __init__(self, page:Page, column:int, is_tail:bool, page_number:int) -> None:
        self.column:int = column
        self.is_tail:bool = is_tail
        self.is_pinned:bool = False
        self.page_number:int = page_number
        self.__page:Page = page
        self.accessed:int = 0 # time when this page was last used

    def is_dirty(self) -> bool:
        return self.__page.is_dirty

    def get_page(self) -> Page:
        """
        Returns the internal page object of the wrapper, and updates the last accessed time
        """
        self.accessed = time_ns()
        return self.__page


class PageDirectory:
    """
    Class abstracting the page directory, provides two functions: retrieve_page and insert_page.
    retrieve_page returns a page object when given the page's column, if it is a tail page, and its page number
    insert_page adds a new page to the page directory

    PageDirectory handles management of the bufferpool
    """
    def __init__(self, table_name:str, database_name:Path, bufferpool_num_pages:int=BUFFERPOOL_SIZE) -> None:
        self.max_pages:int = bufferpool_num_pages
        self.bufferpool:list[PageWrapper] = []
        # print(table_name, database_name)
        self.file_manager = FileManager(table_name, database_name)
        self.num_pages:int = 0

    def save_all(self) -> None:
        """
        Saves all pages in bufferpool to disc. Used when table is closed.
        """
        for page in self.bufferpool:
            if page.is_dirty():
                self.__save_page(page)

    def __save_page(self, page:PageWrapper) -> None:
        """
        Saves the input page to disc
        """
        self.file_manager.page_to_file(page)

    def __load_page(self, column:int, is_tail:bool, page_number:int) -> PageWrapper|None:
        """
        Loads the desired page from disc
        """
        page = self.file_manager.file_to_page(column, is_tail, page_number)
        if page is not None:
            return page
        else:
            # faild to load file
            return None

    def __sort_bufferpool(self) -> None:
        """
        Sorts the bufferpool with Python's builtin Timsort algorithm.
        """
        # most recent == largest number, so sort -accessed will place the most recent at index 0
        self.bufferpool.sort(key=lambda x: -x.accessed)

    def swap_page(self, page: Page, column:int, is_tail:bool, page_number:int) -> None:
        """
        Swaps the page with given (column, is_tail, page_number) for the passed page argument.
        Used for updating an existing page with a consolidated page after merge operation.
        """
        # code for substituting page
        # code for checking if the page is in the bufferpool, because that would also need to be updated
        # NOTE: The bufferpool will be static, as the page_dir is blocked during this process
        is_buffered = any(buffered_page.column==column and buffered_page.is_tail==is_tail and buffered_page.page_number==page_number for buffered_page in self.bufferpool)
        #If in bufferpool
        if is_buffered==True:
            self.bufferpool = list(map(lambda x: page if x.column==column and x.is_tail==is_tail and x.page_number==page_number else x, self.bufferpool))
        self.__save_page(PageWrapper(page, column, is_tail, page_number))


    def retrieve_page(self, column:int, is_tail:bool, page_number:int, update_bufferpool:bool=True) -> Page | None:
        """
        Returns the desired page, if it is in the bufferpool it will be returned directly, otherwise it will be loaded into the bufferpool, then it will be returned.
        """
        if not update_bufferpool:
            # return the page from disk
            pagewrapper = self.__load_page(column, is_tail, page_number)
            if pagewrapper is not None:
                return pagewrapper.get_page()
            else:
                return None
            
        # check all items in bufferpool for target page
        for page in self.bufferpool:
            if page.is_tail == is_tail and page.column == column and page.page_number == page_number:
                return page.get_page()
        # page was not in bufferpool, load it
        # load page from disc
        pagewrapper = self.__load_page(column, is_tail, page_number)
        if pagewrapper is not None:
            # evict least recently used page, if bufferpool is full
            if len(self.bufferpool) >= self.max_pages and self.bufferpool[-1].is_dirty():
                # only save the page if it is dirty (it has been written to)
                self.__save_page(self.bufferpool[-1])
            # replace evicted page with loaded page
            if len(self.bufferpool) < self.max_pages:
                # add loaded page to bufferpool, since it is not full
                self.bufferpool.append(pagewrapper)
            else:
                # replace evicted page with loaded page, since bufferpool is full
                self.bufferpool[-1] = pagewrapper
            # access page object in wrapper
            page = pagewrapper.get_page()
            # sort bufferpool, pagewrapper should be at index 0 after
            self.__sort_bufferpool()
            # return page
            return page
        else:
            # page was not found
            return None

    def insert_page(self, page:Page, column:int, is_tail:bool, page_number:int):
        """
        Adds a page to the PageDirectory, it may be sent directly to disc
        """
        # save new page directly to disc
        self.__save_page(PageWrapper(page, column, is_tail, page_number))


class FileManager:
    def __init__(self, table_name:str, database_name:Path):
        self.database_name = database_name
        self.table_name:str = table_name

    def get_page_number(self, is_tail:bool) -> int:
        """
        Finds the largest page number for a given page type on disk
        """
        if is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        # check from the table's directory within the database
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}")
        # sort RID column, every record has an RID so this column is guarantied to be >= the size of any other column
        all_pages = sorted(file_name.glob(f"{istail_str}_col0_*.bin"))
        if len(all_pages) > 0:
            # the largest page number is at the last element
            _, _, i = str(all_pages[-1].stem).split('_')
            # return the page number
            return int(i)
        elif is_tail:
            # default tail page number
            return -1
        else:
            # default base page number
            return -1


    def file_to_page(self, column:int, is_tail:bool, page_number:int) -> PageWrapper|None:
        """
        Reads a previously saved page from disk, returns the PageWrapper for the page or None if the page could not be found.
        """
        if is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}", f"{istail_str}_col{column}_{page_number}.bin")
        # check if path exists
        # print(str(file_name))
        if file_name.exists():
            page = Page()
            saved_data = bytearray(file_name.read_bytes())
            # cut out saved num_records, it is same length as FIXED_PARTIAL_RECORD_SIZE
            page.num_records = bytearray_to_int(saved_data[:FIXED_PARTIAL_RECORD_SIZE])
            # the rest of the data is the record data
            page.data = saved_data[FIXED_PARTIAL_RECORD_SIZE:]
            page_wrapper = PageWrapper(page, column, is_tail, page_number)
            return page_wrapper
        else:
            # did not find page
            # print(f"No file to load col{column}_{istail_str}_{page_number}.bin")
            return None

    def page_to_file(self, page:PageWrapper):
        """Writes page_wrapper information to corresponding file"""
        if page.is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}", f"{istail_str}_col{page.column}_{page.page_number}.bin")
        # check if path exists
        # print(f"writing file::::: {file_name}")
        if not file_name.parent.exists():
            # make the file path if it doesn't exist
            file_name.parent.mkdir(parents=True)
        # save the current number of records in the page as well
        extra_data = int_to_bytearray(page.get_page().num_records)
        # write the binary file
        file_name.write_bytes(extra_data + page.get_page().data)

    def delete_file(self, column: int, is_tail:bool, page_number:int):
        """Removes file specified by column, is_tail, and page_number"""
        if is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}", f"{istail_str}_col{column}_{page_number}.bin")
        #Delete file if it exists
        if file_name.exists():
            file_name.unlink()

    def delete_files(self):
        """Removes all files within this table, as well as the corresponding directory"""
        dir_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}")
        #If the table exists, delete all of files within it
        if dir_name.exists():
            rmtree(dir_name)

if __name__ == "__main__":
    """
    Test saving/loading
    """
    test_page = Page()
    # write data to test page
    l = 415
    data: list[int] = [0]*l
    for i in range(l):
        test_page.write_direct(int_to_bytearray(56 * i))
        data[i] = 56*i
    # make file manager object
    fm = FileManager("test", Path("TestDB"))
    page_wrapper = PageWrapper(test_page, 0, False, 0)
    num_records = page_wrapper.get_page().num_records
    # save test page to disc
    fm.page_to_file(page_wrapper)
    # load test page from disc
    new_pagewrapper = fm.file_to_page(0, False, 0)
    # compare data, with loaded data
    if new_pagewrapper is not None:
        num_records_load = new_pagewrapper.get_page().num_records
        print("Assert num_records (save){} == num_records (load){} :: {}".format(num_records, num_records_load, num_records == num_records_load))
        test_data = True
        load_data = [0]*l
        for i, x in enumerate(data):
            load_data[i] = bytearray_to_int(new_pagewrapper.get_page().retrieve_direct(i))
            if load_data[i] != x:
                test_data = False
        print(f"Assert save_data == load_data :: {test_data}\nsave_data :: {data}\nload_data :: {load_data}")
    else:
        print("Failed to load file column 0, base, page 0")
