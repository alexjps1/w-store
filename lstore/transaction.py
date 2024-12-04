from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import debug_print as print
from lstore.lock_manager import LockManager

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.is_exclusive = False
        self.queries = []
        self.lock_manager: LockManager | None = None
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        if self.lock_manager is None:
            self.lock_manager = table.lock_manager

        self.queries.append((query, args))
        if query.__name__ == "update" or query.__name__ == "delete" or query.__name__ == "insert":
            self.is_exclusive = True
        self.results = []
        # use grades_table for aborting


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        if self.lock_manager is None:
            return self.abort(False)

        if not self.lock_manager.request_table_lock(self.is_exclusive):
            return self.abort(False)

        for query, args in self.queries:
            result = query(*args)
            self.results.append(result)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(True)
        return self.commit()


    def abort(self, holds_lock):
        #TODO: do roll-back and any other necessary operations

        # Due to the granularity of locking needing to take place on the Table level,
        # transactions should not make any write queries unless the whole table has been aquired
        if holds_lock:
            self.lock_manager.release_table_lock(self.is_exclusive)
        return False


    def commit(self):
        # TODO: commit to database
        self.lock_manager.release_table_lock(self.is_exclusive)
        return True
