from typing import List, Tuple, Union, NewType

RID = NewType('RID', int)

class HashtableIndex:
    def __init__(self):
        self.hashtable:dict[int,List[RID]] = {}
        
    def __str__(self):
        print(self.hashtable.items())
        
    # public methods

    def insert(self, key: int, rid: RID, abs_ver=0, prev_ver_key=None) -> None:
        #Create list for key, or append RID to existing list
        if key not in self.hashtable:
            self.hashtable[key] = [ rid ]
        else:
            self.hashtable[key].append(rid)

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
    
    def update(self, new_val:int, curr_val:int, rid:RID) -> None:
        if new_val==curr_val:
            return
        #Move RID to new list, remove from original list
        if rid in self.hashtable[curr_val]:
            #Make new list for RID if not existing
            if new_val not in self.hashtable:
                self.hashtable[new_val] =  [ rid ]
            #Else add RID to existing list
            else:
                self.hashtable[new_val].append(rid)
            self.hashtable[curr_val].remove(rid)
            #Delete RID list if empty
            if self.hashtable[curr_val]==[]:
                del self.hashtable[curr_val]
        else:
            raise KeyError(f"The RID {rid} is not in the index for value {curr_val}")

    def delete(self, key: int, rid: RID):
        """Remove RID with key, return True if RID exists, False otherwise"""
        #If RID in the list
        if rid in self.hashtable[key]:
            #Remove from list, delete list if empty
            self.hashtable[key].remove(rid)
            if self.hashtable[key]==[]:
                del self.hashtable[key]
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
            result += self.hashtable[key_start]
            key_start += 1
        return result

    def __get_relative_entry_version(self, base_entry: "TreeEntry") -> int:
        pass

    def version_query(self, key: int, rel_ver: int) -> List[RID]:
        #Only called when rel_ver==0
        if key in self.hashtable:
            return self.hashtable[key]
        return []

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


if __name__=="__main__":
    index = HashtableIndex()

    #Test queries
    for i in range(1000):
        index.insert(i%100, i)
    print(index.point_query(0))
    print(index.point_query(5))
    print(index.range_query(0, 2))


    #Testing Update
    index.update(5, 0, 0)              #move rid 0 to list for 5
    index.update(5, 0, 100)             #movie rid 100 to list for 5
    index.update(101, 0, 200)

    print(index.point_query(101))
    print(index.point_query(0))
    print(index.point_query(5))

    for i in range(7):
        index.update(5, 0, 300+(i*100))
    print(index.point_query(5))
    try:
        print(index.hashtable[0])
    except KeyError:
        print("There is no key-value pair for: 0")

    #Test delete
    index.delete(1, 1)
    print(index.point_query(1))
    for i in range(1, 10):
        index.delete(1, (i*100)+1)

    try:
        print(index.hashtable[1])
    except KeyError:
        print("There is no key-value pair for: 1")

