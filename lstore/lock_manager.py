from threading import Lock
from lstore.config import debug_print as print

INDEX = 0
PAGE_DIR = 1
LOCK_MANAGER = 2

class LockManager:
    def __init__(self):
        """Contains mapping for records to locks, as well as locks
        for important data structures shared by each worker thread"""
        self.lock_manager_lock = Lock()
        self.table_exclusive_access: bool = False
        self.table_shared_access_counter: int = 0

    def request_table_lock(self, is_exclusive: bool) -> bool:
        """
        Attempt to acquire a lock on the table.
        Returns True if lock granted and False if not.
        """
        # wait to access the lock manager
        self.lock_manager_lock.acquire(blocking=True)
        lock_acquired = False
        # this is now the only thread looking at the lock manager
        if is_exclusive:
            if not (self.table_shared_access_counter or self.table_exclusive_access):
                self.table_exclusive_access = True
                lock_acquired = True
                # print("Xlock acquired")
        elif not self.table_exclusive_access:
            self.table_shared_access_counter += 1
            lock_acquired = True
            # print(f"Slock acquired ({self.table_shared_access_counter} threads reading)")
        self.lock_manager_lock.release()
        return lock_acquired

    def release_table_lock(self, is_exclusive: bool) -> None:
        """
        Release the table lock.
        """
        # wait to access the lock manager
        self.lock_manager_lock.acquire(blocking=True)
        # this is now the only thread looking at the lock manager
        if is_exclusive:
            self.table_exclusive_access = False
            # print(f"Xlock released")
        else:
            self.table_shared_access_counter -= 1
            assert self.table_exclusive_access >= 0
            # print(f"Slock released ({self.table_shared_access_counter} threads reading)")
        self.lock_manager_lock.release()


"""
    def get_record_lock(self, RID:int, is_exclusive:bool) -> bool:
        # Acquires lock assoicated with RID
        # Output: Returns True if lock was acquired, False if not
        if is_exclusive:
            #If shared lock being used, fail and return false
            if RID in self.shared_record_locks:
                return False                            #Simply check if there is shared lock
            #Else try to acquire exclusive lock
            if RID not in self.exclusive_record_locks:
                self.exclusive_record_locks[RID] = Lock()
            return self.exclusive_record_locks[RID].acquire(blocking=False)
        else:
            #If exclusive lock being used, fail and return false
            if RID in self.exclusive_record_locks:
                if self.exclusive_record_locks[RID].locked():
                    return False
            #Else try to acquire shared lock
            if RID not in self.shared_record_locks:
                self.shared_record_locks[RID] = Lock()
            return not self.shared_record_locks[RID].locked()       #Return True if shared lock not blocked

    def release_record_lock(self, RID:int, is_exclusive:bool):
        # Delete Record Lock if in mapping
        if is_exclusive:
            if RID in self.exclusive_record_locks:
                del self.exclusive_record_locks[RID]
        else:
            if RID in self.shared_record_locks:
                del self.shared_record_locks[RID]

    def is_acquired(self, data_struct_id:int) -> bool:
        # Returns whether the shared lock is acquired
        if data_struct_id==INDEX:
            return self.index_lock.locked()
        elif data_struct_id==PAGE_DIR:
            return self.page_dir_lock.locked()
        elif data_struct_id==LOCK_MANAGER:
            return self.lock_manager_lock.locked()
        else:
            raise Exception("Invalid Data Structure ID given for check!")

    def acquire_shared_lock(self, data_struct_id:int):
        # Acquires shared lock and returns True on success, else returns False
        if data_struct_id==INDEX:
            return self.index_lock.acquire(blocking=False)
        elif data_struct_id==PAGE_DIR:
            return self.page_dir_lock.acquire(blocking=False)
        elif data_struct_id==LOCK_MANAGER:
            return self.lock_manager_lock.acquire(blocking=False)
        else:
            raise Exception("Invalid Data Structure ID given for acquisition!")

    def release_shared_lock(self, data_struct_id:int):
        # Releases shared lock
        if data_struct_id==INDEX:
            self.index_lock.release()
        elif data_struct_id==PAGE_DIR:
            self.page_dir_lock.release()
        elif data_struct_id==LOCK_MANAGER:
            self.lock_manager_lock.release()
        else:
            raise Exception("Invalid Data Structure ID given for release!")

if __name__=="__main__":
    rid1 = 1
    rid2 = 2
    lock_manager = LockManager()
    lock_manager.add_lock(rid1)
    lock_manager.add_lock(rid2)
    print(lock_manager.get_record_lock(rid1))
    print(lock_manager.get_record_lock(rid1))
    lock_manager.release_record_lock(rid1)
    print(lock_manager.get_record_lock(rid1))

    print("Is acquired?", lock_manager.is_acquired(INDEX))
    print(lock_manager.acquire_shared_lock(INDEX))
    lock_manager.release_shared_lock(INDEX)

    print(lock_manager.acquire_shared_lock(PAGE_DIR))
    print("Is acquired?", lock_manager.is_acquired(PAGE_DIR))
    lock_manager.release_shared_lock(PAGE_DIR)

    print(lock_manager.acquire_shared_lock(LOCK_MANAGER))
    lock_manager.release_shared_lock(LOCK_MANAGER)
    print("Is acquired?", lock_manager.is_acquired(LOCK_MANAGER))
"""
