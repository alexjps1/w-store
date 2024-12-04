from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import debug_print as print
from threading import Thread

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.t = Thread(target=self.__run)

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # create a thread and call __run
        self.t.run()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.t.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

