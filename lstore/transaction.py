from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import debug_print as print
from lstore.lock_manager import request_table_lock, release_table_lock
class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.is_exclusive = False
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if query.__name__ == "update" or query.__name__ == "delete" or query.__name__ == "insert":
            self.is_exclusive = True
        self.results = []
        # use grades_table for aborting


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        if request_table_lock(self.is_exclusive):
            return self.abort()

        for query, args in self.queries:
            result = query(*args)
            self.results.append(result)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()


    def abort(self):
        #TODO: do roll-back and any other necessary operations

        # Due to the granularity of locking needing to take place on the Table level,
        # transactions should not make any write queries unless the whole table has been aquired
        release_table_lock(self.is_exclusive)
        return False


    def commit(self):
        # TODO: commit to database
        release_table_lock(self.is_exclusive)
        return True
