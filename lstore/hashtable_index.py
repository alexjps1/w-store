from typing import List, Tuple, Union, NewType
from lstore.config import DATABASE_DIR
from lstore.config import debug_print as print
from pathlib import Path
import json

RID = NewType('RID', int)

class HashtableIndex:
    def __init__(self):
        self.hashtable:dict[int,List[RID]] = {}
        self.rid_val_map:dict[RID, int] = {}
        
    def __str__(self):
        # print(str(self.hashtable.items()))
        pass
        
    # public methods

    def insert(self, key: int, rid: RID, abs_ver=0, prev_ver_key=None) -> None:
        #Create list for key, or append RID to existing list
        if key not in self.hashtable:
            self.hashtable[key] = [ rid ]
        else:
            self.hashtable[key].append(rid)

        self.rid_val_map[rid] = key

    """
    def update(self, new_val: int, curr_val: int, rid: RID) -> None:
        if new_val==curr_val:
            return
        #Move RID to new list, remove original list
        for rid_i in self.hashtable[curr_val]:
            #Check if new_val list exists, else create one
            if new_val not in self.hashtable:
                self.hashtable[new_val] = [ rid_i ]
            #Add rid in list if not already in it
            elif rid_i not in self.hashtable[new_val]:
                self.hashtable[new_val].append(rid_i)
        del self.hashtable[curr_val]"""
    
    def update(self, new_val:int, rid:RID) -> None:
        #Check if RID in table
        if rid not in self.rid_val_map:
            raise KeyError(f"RID {rid} is not in the hashtable")
        
        #If same value, do nothing
        if new_val==self.rid_val_map[rid]:
            return
        #Move RID to new list, remove from original list
        #Check if curr_val in hashtable
        if rid in self.hashtable[self.rid_val_map[rid]]:
            #Make new list for RID if not existing
            if new_val not in self.hashtable:
                self.hashtable[new_val] =  [ rid ]
            #Else add RID to existing list
            else:
                self.hashtable[new_val].append(rid)
            self.hashtable[self.rid_val_map[rid]].remove(rid)

            #Delete RID list if empty
            if self.hashtable[self.rid_val_map[rid]]==[]:
                del self.hashtable[self.rid_val_map[rid]]
            #Change reverse index
            self.rid_val_map[rid] = new_val
        else:
            raise KeyError(f"The RID {rid} is not in the index")

    def delete(self, key: int, rid: RID):
        """Remove RID with key, return True if RID exists, False otherwise"""
        #If RID in the list
        if rid in self.hashtable[key]:
            #Remove from list, delete list if empty
            self.hashtable[key].remove(rid)
            if self.hashtable[key]==[]:
                del self.hashtable[key]
            del self.rid_val_map[rid]
            return True
        return False

    def point_query(self, key: int) -> List[RID]:
        #Return list of RIDs associated with key
        if key in self.hashtable:
            return self.hashtable[key]
        return []

    def range_query(self, key_start: int, key_end: int) -> List[RID]:
        #Return list of RIDs within range of key values
        result = []
        while key_start<=key_end:
            result += self.point_query(key_start)
            key_start += 1
        return result

    def __get_relative_entry_version(self, base_entry: "TreeEntry") -> int:
        pass

    def version_query(self, key: int, rel_ver: int) -> List[RID]:
        #Only called when rel_ver==0
        if key in self.hashtable:
            return self.hashtable[key]
        return []
    
    def save_index(self, path:str, col_num:int) -> None:
        """Path goes up to table_name"""
        index_path = Path(path, "index", f"col{col_num}")
        if not index_path.exists():
            index_path.mkdir(parents=True)
        #Save Hash
        with open(Path(index_path, f"hashmap_index.json"), "w") as file_name:
            json.dump(self.hashtable, file_name, indent=4)
            file_name.close()
        #Save Reverse Hash
        with open(Path(index_path, "hashmap_reverse.json"), "w") as file_name:
            json.dump(self.rid_val_map, file_name)
            file_name.close()

    def keystoint(self, x):
        return {int(k): v for k, v in x.items()}

    def load_index(self, path:str, col_num:int) -> None:
        """Path goes up to table_name"""
        index_path = Path(path, "index", f"col{col_num}")
        hash_path = Path(index_path, "hashmap_index.json")
        reverse_path = Path(index_path, "hashmap_reverse.json")
        if hash_path.exists():
            with open(hash_path, "r") as file_name:
                self.hashtable = json.load(file_name, object_hook=self.keystoint)
                file_name.close()
        else:
            raise FileNotFoundError(f"Error: Hashmap for column {col_num} not on disk")
        if reverse_path.exists():
            with open(reverse_path, "r") as file_name:
                self.rid_val_map = json.load(file_name, object_hook=self.keystoint)
                file_name.close()
        else:
            raise FileNotFoundError(f"Error: Reverse Hashmap for column {col_num} not on disk")

"""
    incomplete methods for key->list[RID,rel_val]  mapping

    def insert(self, key: int, rid: RID, abs_ver=0, prev_ver_key=None) -> None:
        #Create list for key, or append RID to existing list
        exists = False
        if key not in self.hashtable:
            self.hashtable[key] = [ (rid, 0) ]
        else:
            self.hashtable[key].append((rid,0))

    def update(self, new_val:int, curr_val:int, rid:RID) -> None:
        if new_val==curr_val:
            return
        #Add RID to new list with incremented rel_val
        versions = []
        for tup in self.hashtable[curr_val]:
            if tup[0]==RID:
                versions.append(tup[1])
        if new_val not in self.hashtable:
            self.hashtable[new_val] = [ (rid, max(versions)+1) ]          #Assumes curr_val exists in hastable

    def delete(self, key: int, rid: RID):
        pass

    def point_query(self, key: int) -> List[RID]:
        versions = self.hashtable[key]
        for tup in versions:
            if tup
        return
    
    def range_query(self, key_start: int, key_end: int) -> List[RID]:
        pass

    def version_query(self, key: int, rel_ver: int) -> List[RID]:
        pass

    """


"""
if __name__=="__main__":
    index = HashtableIndex()

    print("___TESTING_PQs___")
    for i in range(100):
        index.insert(i%10, i)
    print(index.point_query(0))
    print(index.point_query(5))
    print(index.range_query(0, 2))
    print(index.rid_val_map)


    print("___TESTING_UPDATE___")
    #Simple updates
    index.update(5, 0)              #rid0->5
    index.update(5, 1)             #rid1->5
    index.update(11, 1)            #rid1->11

    print(index.point_query(11))
    print(index.point_query(0))
    print(index.point_query(5))
    print(index.rid_val_map)

    for i in range(10):
        index.update(5, 10*i)
    print(index.point_query(0))
    print(index.point_query(5))
    print(index.rid_val_map)
    try:
        print(index.hashtable[0])
    except KeyError:
        print("There is no key-value pair for: 0")

    #Test delete
    print("___TESTING_DELETE___")
    index.delete(1, 1)
    print(index.point_query(1))
    for i in range(1, 10):
        index.delete(1, (i*10)+1)

    try:
        print(index.hashtable[1])
    except KeyError:
        print("There is no key-value pair for: 1")
    print(index.rid_val_map)

    print("___TESTING_SELECT_VERSION___")
    print(index.point_query(2))
    print(index.version_query(2,0))
    index.save_index(Path(DATABASE_DIR, "CS451", "Grades"), 1)

    print("____TESTING_SAVE/LOAD____")
    index2 = HashtableIndex()
    index2.load_index(Path(DATABASE_DIR, "CS451", "Grades"), 1)
    print(index2.hashtable)
    print(index2.rid_val_map)
    print(index.hashtable==index2.hashtable)

"""