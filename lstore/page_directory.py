from lstore.page import Page
from lstore.config import BUFFERPOOL_SIZE
from lstore.file_manager import FileManager
from pathlib import Path
from time import time_ns
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
        self.file_manager = FileManager(table_name, database_name)
        self.num_pages:int = 0

    def save_all(self) -> None:
        """
        Saves all pages in bufferpool to disc. Used when table is closed.
        """
        for page in self.bufferpool:
            self.__save_page(page)

    def __save_page(self, page:PageWrapper) -> None:
        """
        Saves the input page to disc
        """
        self.file_manager.page_to_file(page)

    def __load_page(self, column:int, is_tail:bool, page_number:int) -> PageWrapper:
        """
        Loads the desired page from disc
        """
        page = self.file_manager.file_to_page(column, is_tail, page_number)
        if page is not None:
            return page
        else:
            # faild to load file
            print(column, is_tail, page_number)
            assert False

    def __sort_bufferpool(self) -> None:
        """
        Sorts the bufferpool with Python's builtin Timsort algorithm.
        """
        # most recent == largest number, so sort -accessed will place the most recent at index 0
        self.bufferpool.sort(key=lambda x: -x.accessed)

    def retrieve_page(self, column:int, is_tail:bool, page_number:int) -> Page:
        """
        Returns the desired page, if it is in the bufferpool it will be returned directly, otherwise it will be loaded into the bufferpool, then it will be returned.
        """
        # check all items in bufferpool for target page
        for page in self.bufferpool:
            if page.is_tail == is_tail and page.column == column and page.page_number == page_number:
                return page.get_page()
        # page was not in bufferpool, load it
        # evict least recently used page
        if not self.bufferpool[-1].is_dirty():
            # only save the page if it is dirty (it has been written to)
            self.__save_page(self.bufferpool[-1])
        # load page from disc
        pagewrapper = self.__load_page(column, is_tail, page_number)
        # replace evicted page with loaded page
        self.bufferpool[-1] = pagewrapper
        # access page object in wrapper
        page = pagewrapper.get_page()
        # sort bufferpool, pagewrapper should be at index 0 after
        self.__sort_bufferpool()
        # return page
        return page

    def insert_page(self, page:Page, column:int, is_tail:bool, page_number:int):
        """
        Adds a page to the PageDirectory, it may be sent directly to disc
        """
        if self.num_pages < self.max_pages:
            # bufferpool is not full
            self.bufferpool.append(PageWrapper(page, column, is_tail, page_number))
            self.num_pages += 1
            self.__sort_bufferpool()
        else:
            # save new page directly to disc
            self.__save_page(PageWrapper(page, column, is_tail, page_number))
            # code for adding new page to bufferpool instead
            # bufferpool is full, evict a page, then add a page
            # self.__save_page(self.bufferpool[-1])
            # # replace evicted page with new one
            # self.bufferpool[-1] = PageWrapper(page, column, is_tail, page_number)
            # self.__sort_bufferpool()
