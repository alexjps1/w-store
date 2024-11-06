from lstore.page import Page
from lstore.page_directory import PageWrapper, PageDirectory
from lstore.config import FIXED_PARTIAL_RECORD_SIZE


class FileManager:
    def __init__(self, table_name):
        self.table_name:str = table_name
    
    def file_to_page(self, column, is_tail, page_number) -> Page:
        """Throw error if file doesn't exist."""
        try:
            if is_tail:
                is_tail = "t"
            else:
                is_tail = "b"
            file_name = f"disk/{self.table_name}/col{column}_{is_tail}_{page_number}.txt"
            print(file_name)
            with open(file_name, "rb") as f:
                bytes_array = f.read()
                page = Page()
                page.write_direct(bytes_array)
                page_wrapper = PageWrapper(page, column, is_tail, page_number)
                #bytes = text.encode()
            return page_wrapper
        except:
            print("Error with saving page")

    def page_to_file(self, page:PageWrapper):
        """Writes page_wrapper information to corresponding file"""
        if page.is_tail:
            is_tail = "t"
        else:
            is_tail = "b"
        file_name = f"disk/{self.table_name}/col{page.column}_{is_tail}_{page.page_number}.txt"        #Num records?
        with open(file_name, "wb") as f:
            #print(page.get_page().data)
            f.write(page.get_page().data)
            f.close()
        #with open(file_name, "w") as f: #needs to be byte file
            
if __name__=="__main__":
    page = Page()
    page_wrapper = PageWrapper(page,1,True,2)       #Create Page Wrapper
    data = [1, 2, 3, 4, 5]
    bytes_array = bytearray(data)                   #Write bytes to page in page wrapper
    page.write_direct(bytes_array)
    manager = FileManager()
    manager.page_to_file(page_wrapper)              #Save page to file
    new = manager.file_to_page(1,True,2)            #Load file to page
    new.get_page().data