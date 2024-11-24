from threading import Lock

INDEX = 0
PAGE_DIR = 1
LOCK_MANAGER = 2

class LockManager:
    def __init__(self):
        """Contains mapping for records to locks, as well as locks
        for important data structures shared by each worker thread"""
        self.record_locks = {}
        #Shared locks are acquired by the main thread, to prevent access
        self.index_lock = Lock()
        self.page_dir_lock = Lock()
        self.lock_manager_lock = Lock()     #May be unecessary

    def add_lock(self, RID):
        """
        Adds a for associated RID to the record->lock mapping
        This should be called alongside insert methods
        """
        self.record_locks[RID] = Lock()

    def get_record_lock(self, RID:int) -> bool:
        """
        Acquires lock assoicated with RID
        Output: Returns True if lock was acquired, False if not
        """
        if RID not in self.record_locks:
            self.record_locks[RID] = Lock()
        return self.record_locks[RID].acquire(blocking=False)
    
    def release_record_lock(self, RID):
        """Delete Record Lock if in mapping"""
        if RID in self.record_locks:
            del self.record_locks[RID]
    
    def is_acquired(self, data_struct_id:int) -> bool:
        """Returns whether the shared lock is acquired"""
        if data_struct_id==INDEX:
            return self.index_lock.locked()
        elif data_struct_id==PAGE_DIR:
            return self.page_dir_lock.locked()
        elif data_struct_id==LOCK_MANAGER:
            return self.lock_manager_lock.locked()
        else:
            raise Exception("Invalid Data Structure ID given for check!")
    
    def acquire_shared_lock(self, data_struct_id:int):
        """Acquires shared lock and returns True on success, else returns False"""
        if data_struct_id==INDEX:
            return self.index_lock.acquire(blocking=False)
        elif data_struct_id==PAGE_DIR:
            return self.page_dir_lock.acquire(blocking=False)
        elif data_struct_id==LOCK_MANAGER:
            return self.lock_manager_lock.acquire(blocking=False)
        else:
            raise Exception("Invalid Data Structure ID given for acquisition!")

    def release_shared_lock(self, data_struct_id:int):
        """Releases shared lock"""
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