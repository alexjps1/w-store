from lstore.page import Page
from lstore.page_directory import PageWrapper
from lstore.config import DATABASE_DIR
from pathlib import Path


class FileManager:
    def __init__(self, table_name:str, database_name:Path):
        self.database_name = database_name
        self.table_name:str = table_name
    
    def file_to_page(self, column:int, is_tail:bool, page_number:int) -> PageWrapper|None:
        """
        Reads a previously saved page from disk, returns the PageWrapper for the page or None if the page could not be found.
        """
        if is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}", f"col{column}_{istail_str}_{page_number}.bin")
        # check if path exists
        if file_name.exists():
            page = Page()
            page.data = bytearray(file_name.read_bytes())
            page_wrapper = PageWrapper(page, column, is_tail, page_number)
            return page_wrapper
        else:
            print("Error with saving page")
            return None

    def page_to_file(self, page:PageWrapper):
        """Writes page_wrapper information to corresponding file"""
        if page.is_tail:
            istail_str = "t"
        else:
            istail_str = "b"
        file_name = Path(DATABASE_DIR, f"{self.database_name}", f"{self.table_name}", f"col{page.column}_{istail_str}_{page.page_number}.bin")
        # check if path exists
        if not file_name.exists():
            # make the file path if it doesn't exist
            file_name.mkdir(parents=True)
        # write the binary file
        file_name.write_bytes(page.get_page().data)
            
if __name__=="__main__":
    page = Page()
    page_wrapper = PageWrapper(page,1,True,2)       #Create Page Wrapper
    data = [1, 2, 3, 4, 5]
    bytes_array = bytearray(data)                   #Write bytes to page in page wrapper
    page.write_direct(bytes_array)
    manager = FileManager("test", Path("test_db"))
    manager.page_to_file(page_wrapper)              #Save page to file
    new = manager.file_to_page(1,True,2)            #Load file to page
    new.get_page().data
