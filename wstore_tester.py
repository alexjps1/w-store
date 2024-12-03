from lstore.db import Database
from lstore.query import Query
from lstore.table import Table
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from pathlib import Path
from enum import Enum
from typing import Callable

DB_NAME = "WstoreTesterDatabase"
ANSWERKEY_FILE = Path("wstore_tester_answerkey.json")
NUM_COL = 5
LOG_LEVEL = 9
NUM_FILLER_TESTS = 1000
NUM_TRANSACTIONS = 100
NUM_THREADS = 8

class QueryType(Enum):
    INSERT = 0
    UPDATE = 1
    SELECT = 2
    SELECT_VERSION = 3
    SUM = 4
    SUM_VERSION = 5
    DELETE = 6


def add_lists(l1, l2):
    assert len(l1) == len(l2)
    return [l1[i] if l2[i] is None else l2[i] for i in range(len(l1))]


class Test:
    """
    Contains a record of a single test and can evaluate its correctness
    run update_result before evaluate
    """
    def __init__(self, message:str, query:QueryType, answer, supress_messages=False, **kwargs):
        self.query = query
        self.message = message
        self.kwargs = kwargs
        self.answer = answer
        self.__result = None
        self.result_updated = False
        self.supress_messages = supress_messages

    def update_result(self, value):
        self.__result = value
        self.result_updated = True

    def evaluate(self) -> bool:
        if self.result_updated:
            try:
                if self.query == QueryType.SELECT or self.query == QueryType.SELECT_VERSION:
                    # fix record lists
                    if len(self.__result) > 0:
                        self.__result = self.__result[0].columns
                if self.answer == self.__result:
                    if not self.supress_messages and LOG_LEVEL > 3: print(f"PASSED:::: {self.message} ::::")
                    return True
                else:
                    if not self.supress_messages and LOG_LEVEL > 4: print(f"FAILED:::: {self.message} ::::\t\tAnswer=={self.answer}\t\tResult=={self.__result}")
                    return False
            except Exception as e:
                if not self.supress_messages and LOG_LEVEL > 6: print(f"#### EXCEPTION {e} in {self.message} ####")
                return False
        else:
            if not self.supress_messages and LOG_LEVEL > 5: print("!!!Attempted to evaluate result but result was not updated!!!")
            return False


def evaluate_correctness(test_cases:list[Test]) -> list[bool]:
    """
    evaluates the correctness of the given test_case list, assuming all test results have been updated.
    """
    correctness = [False]*len(test_cases)
    for i, test in enumerate(test_cases):
        correctness[i] = test.evaluate()
    return correctness

def gen_filler_tests(num_records:int) -> list[Test]:
    tests = []
    for i in range(num_records):
        tests.append(Test(f"filler insert {i}", QueryType.INSERT, True, True, columns=[6000000 + i, 6, 6, 6, 6]))
    for i in range(num_records):
        tests.append(Test(f"filler update {i}", QueryType.INSERT, True, True, primary_key=6000000 + i, columns=[None, 16, None, 4, 64]))
    return tests


def create_tests(is_loaded=False, filler_tests=False) -> list[Test]:
    """
    The evaluation layer initializes the query test sweat with the queries and their expected results.
    - insert on:
        * valid/invalid columns
        * null/non-null values
        * unique/non-unique values
    - update on:
        * valid/invalid columns
        * null/non-null values
        * unique/non-unique values
            * primary keys/data columns
    - select on:
        * base records/tail records:
            + are in table/aren't in table
            + primary keys/data columns
            + single updates/multi updates
            + first/last element
        * versions
    - sum on:
        * invalid start
        * invalid end
        * base records
        * tail records
        * versions
        * unique/non-unique start/end values
    - delete on:
        * valid/invalid columns
        * unique/non-unique values
        * data in table/data not in table
        * base records/tail records
        * primary key updates
    """
    # all tests
    tests:list[Test] = []
    if filler_tests:
        tests = gen_filler_tests(NUM_FILLER_TESTS)
    # test normal inserts
    base_records = [
            [920000, 0, 0, 0, 0],
            [920001, 1, 1, 1, 1],
            [920002, 1, 2, 3, 4],
            [920003, 4, 3, 2, 1],
            [920004, 3, 0, 7, 9],
            [920005, 1, 5, 8, 5],
            [920006, 9, 2, 8, 0],
            [920007, 1, 5, 5, 5],
            [920008, 1, 0, 2, 5],
            [920009, 4, 5, 8, 8]
            ]
    if not is_loaded:
        for i, record in enumerate(base_records):
            tests.append(Test(f"insert base record {i}", QueryType.INSERT, True, columns=record))
    # test bad inserts
    bad_base_records = [
            [],
            [None]*5,
            [None, 5, 5, 5, 5],
            [9999, 1],
            [9998, 0, 1, 2, 3, 4, 5],
            [920000, 6, 6, 4, 4]
            ]
    if not is_loaded:
        for i, record in enumerate(bad_base_records):
            tests.append(Test(f"insert bad base record {i}", QueryType.INSERT, False, columns=record))
    # test normal updates
    tail_records = [
            [None, 99, None, None, None],
            [None, 90, None, 90, None],
            [120000, 0, 0, 0, 0], # full update, new primary key
            [None, 25, None, 19, None],
            [None, None, 33, None, 98],
            [80, None, None, None, None], # update primary key only
            [None, 81, None, None, None],
            [None, None, 82, None, None],
            [None, None, None, 83, None],
            [None, None, None, None, 84]
            ]
    if not is_loaded:
        for i in range(len(tail_records)):
            tests.append(Test(f"append tail record {i}", QueryType.UPDATE, True, primary_key=920000+i, columns=tail_records[i]))
    # test bad updates
    bad_tail_records = [
            [],
            [None]*5,
            [920006, None, 555, None, None], # update to existing primary key
            [920002, 999, 666, 333, 111] # this is valid but we will delete it later, in an invalid way
            ]
    if not is_loaded:
        tests.append(Test(f"append bad tail - empty list -", QueryType.UPDATE, False, primary_key=920004, columns=bad_tail_records[0]))
        tests.append(Test(f"append bad tail - Null list -", QueryType.UPDATE, False, primary_key=920004, columns=bad_tail_records[1]))
        tests.append(Test(f"append bad tail - non-unique primary key -", QueryType.UPDATE, False, primary_key=920001, columns=bad_tail_records[2]))
        tests.append(Test(f"append tail that will cause a conflict with a delete later", QueryType.UPDATE, True, primary_key=920009, columns=bad_tail_records[3]))
    # test select
    tests.append(Test(f"select for updated record", QueryType.SELECT, add_lists(base_records[0], tail_records[0]), search_key=920000, search_key_index=0, projected_columns_index=[1]*5)) # [920000, 99, 0, 0, 0]
    tests.append(Test(f"select for updated primary key", QueryType.SELECT, add_lists(base_records[2], tail_records[2]), search_key=120000, search_key_index=0, projected_columns_index=[1]*5)) # [120000, 0, 0, 0, 0]
    # test bad select
    tests.append(Test(f"select for primary key that is not in table", QueryType.SELECT, [], search_key=999999, search_key_index=0, projected_columns_index=[1]*5)) # key not in table
    tests.append(Test(f"select for with search key index out of range", QueryType.SELECT, [], search_key=920000, search_key_index=9, projected_columns_index=[1]*5)) # search_key_index is out of range
    tests.append(Test(f"select with projected_columns_index too long", QueryType.SELECT, [], search_key=920000, search_key_index=0, projected_columns_index=[1]*7)) # projected_columns_index is too long
    # test select_version
    tests.append(Test(f"select version for updated record", QueryType.SELECT_VERSION, add_lists(base_records[0], tail_records[0]), search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=0)) # [920000, 99, 0, 0, 0]
    tests.append(Test(f"select version for base record", QueryType.SELECT_VERSION, base_records[0], search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=-1)) # base record [920000, 0, 0, 0, 0]
    # test bad select_version
    tests.append(Test(f"select version with relative_version out of range -2", QueryType.SELECT_VERSION, base_records[0], search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=-2)) # passed base record [920000, 0, 0, 0, 0]
    tests.append(Test(f"select version with relative_version out of range -3", QueryType.SELECT_VERSION, base_records[5], search_key=920005, search_key_index=0, projected_columns_index=[1]*5, relative_version=-3)) # primary key was updated to 80, but this version should return the base record [920005, 1, 5, 8, 5]
    # test sum
    tests.append(Test(f"sum with continuous sorted range no key changes", QueryType.SUM, 96, start_range=920006, end_range=920008, aggregate_column_index=3)) # values in order some were updated, 8 + 5 + 83 = 96
    tests.append(Test(f"sum with unsorted range and some key changes", QueryType.SUM, 710, start_range=0, end_range=920006, aggregate_column_index=2)) # values not in order some were updated and start key is not in table but range is still valid, 0 + 1 + 0 + 3 + 33 + 5 + 2 + 666 = 710
    # test bad sum
    tests.append(Test(f"sum with range not in table", QueryType.SUM, 0, start_range=999989, end_range=999999, aggregate_column_index=3)) # key range not in table, result should be 0
    # test sum_version
    tests.append(Test(f"sum version on base records -1", QueryType.SUM_VERSION, 19, start_range=920003, end_range=920008, aggregate_column_index=1, relative_version=-1)) # 1 tail for each so summing on base records some keys have been updated, 4 + 3 + 1 + 9 + 1 + 1 = 19
    # test bad sum_version
    tests.append(Test(f"sum version on base records, version exceeds history -2", QueryType.SUM_VERSION, 15, start_range=920006, end_range=920008, aggregate_column_index=3, relative_version=-2)) # version exceeds base so we are summing the base records, 8 + 5 + 2 = 15
    tests.append(Test(f"sum version on base records, version exceeds history -4", QueryType.SUM_VERSION, 19, start_range=920003, end_range=920008, aggregate_column_index=1, relative_version=-4)) # same as a previous test, but trying to look to far back
    tests.append(Test(f"sum version with range not in table, version -1", QueryType.SUM_VERSION, 0, start_range=999989, end_range=999999, aggregate_column_index=3, relative_version=-1)) # same as a previous test but with sum_version

    # test delete
    if not is_loaded:
        tests.append(Test(f"delete a record in table", QueryType.DELETE, True, primary_key=920000))
        # test bad deletes
        # not allowed since this will uncover primary key 920002, and 920009 was updated to 920002
        tests.append(Test(f"delete a record that will cause a primary key to be non-unique", QueryType.DELETE, False, primary_key=120000))
        # not a primary key
        tests.append(Test(f"delete a record that is not in table", QueryType.DELETE, False, primary_key=0))
        # test key was deleted successfully
        tests.append(Test(f"select for record that was deleted", QueryType.SELECT, [], search_key=920000, search_key_index=0, projected_columns_index=[1]*5 ))
    else:
        # test that values were saved
        expected_table = [add_lists(base_records[i], tail_records[i]) for i in range(len(base_records))]
        expected_table[-1] = add_lists(expected_table[-1], bad_tail_records[3])
        del expected_table[0] # this one was deleted
        for i, value in enumerate(expected_table):
            tests.append(Test(f"check total base+tail record {i} was saved", QueryType.SELECT, value, search_key=value[0], search_key_index=0, projected_columns_index=[1]*5))
        # test key is still deleted
        tests.append(Test(f"check deleted record is still deleted", QueryType.SELECT, [], search_key=920000, search_key_index=0, projected_columns_index=[1]*5 ))
        for i, base in enumerate(base_records):
            if i == 0:
                # i==0 is 920000 that was deleted
                pass
            else:
                # check base records
                tests.append(Test(f"check base record {i} was saved", QueryType.SELECT_VERSION, base, search_key=920000 + i, search_key_index=0, projected_columns_index=[1]*5, relative_version=-1))

    return tests


class DatabaseLayer:
    """
    Table layer sets up middle level tests that are independent of both tests at a lower and higher layers.
    Tests:
    - single page (1 operation)
    - multi page (1000 operations)
    - init database
    - don't init database
    - new table
    - load table
    - serial execution
    - parallel (transaction worker)
    - transactions (assignment > 2)
    - non-transactions (assignment <= 2) [not compatible with parallel]
    """
    def __init__(self):
        pass

    def run(self):
        # start tests
        self.page_layer()

    def page_layer(self):
        # single page test
        self.database_init_layer("single-page", False)
        # multi-page test
        self.database_init_layer("multi-page", True)

    def database_init_layer(self, layer_name:str, filler_tests:bool):
        # init database test
        self.load_table_layer(f"{layer_name}|init-database", self.init_database, filler_tests)
        # non-init database test
        self.load_table_layer(f"{layer_name}|non-init-database", self.noinit_database, filler_tests)

    def load_table_layer(self, layer_name:str, db_init_function:Callable[[], Database], filler_tests:bool):
        # new table test
        self.concurrency_layer(f"{layer_name}|new-table", self.new_table, db_init_function, filler_tests)
        # load table test
        self.concurrency_layer(f"{layer_name}|load-table", self.load_table, db_init_function, filler_tests)

    def concurrency_layer(self, layer_name:str, gen_query_function:Callable[[Database, str], Query], db_init_function:Callable[[], Database], filler_tests:bool):
        # serial test
        self.transaction_layer(f"{layer_name}|serial", gen_query_function, db_init_function, filler_tests)
        # parallel test (transaction workers)
        self.transaction_layer(f"{layer_name}|parallel", gen_query_function, db_init_function, filler_tests)

    def transaction_layer(self, layer_name:str, gen_query_function:Callable[[Database, str], Query], db_init_function:Callable[[], Database], filler_tests:bool):
        """
        # ### serial tests only ###
        """
        if not layer_name.__contains__("parallel"):
            # init test system
            sub_layer_name = f"{layer_name}|queries"
            table_name = sub_layer_name.replace("new-table", "table").replace("load-table", "table")
            # init database
            db = db_init_function()
            query_one = gen_query_function(db, table_name)
            if LOG_LEVEL > 8: print(f"{'#'*10}\nStarting {sub_layer_name}\n{'#'*10}")
            is_loaded = layer_name.__contains__("load-table")
            # generate test cases
            testcase_set_one = create_tests(is_loaded, filler_tests)
            # non-transaction test (just queries)
            for test_case in testcase_set_one:
                # run queries and update results
                q = test_case.query
                if q == QueryType.INSERT:
                    try:
                        result = query_one.insert(*test_case.kwargs["columns"])
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} INSERT :: Exception=={e}")
                elif q == QueryType.UPDATE:
                    try:
                        result = query_one.update(test_case.kwargs["primary_key"], *test_case.kwargs["columns"])
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} UPDATE :: Exception=={e}")
                elif q == QueryType.SELECT:
                    try:
                        result = query_one.select(**test_case.kwargs)
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} SELECT :: Exception=={e}")
                elif q == QueryType.SELECT_VERSION:
                    try:
                        result = query_one.select_version(**test_case.kwargs)
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} SELECT_VERSION :: Exception=={e}")
                elif q == QueryType.SUM:
                    try:
                        result = query_one.sum(**test_case.kwargs)
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} SUM :: Exception=={e}")
                elif q == QueryType.SUM_VERSION:
                    try:
                        result = query_one.sum_version(**test_case.kwargs)
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} SUM_VERSION :: Exception=={e}")
                else: # delete
                    try:
                        result = query_one.delete(**test_case.kwargs)
                        test_case.update_result(result)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} DELETE :: Exception=={e}")
            correctness_one = evaluate_correctness(testcase_set_one)
            if False in correctness_one:
                if LOG_LEVEL >= 0: print(f"!!!!! FAILED Test {sub_layer_name} !!!!!")
            else:
                if LOG_LEVEL >= 0: print(f"::::: PASSED Test {sub_layer_name} :::::")
            db.close()
        """
        # ### parallel and serial tests ###
        """
        # init test system
        sub_layer_name = f"{layer_name}|transaction"
        table_name = sub_layer_name.replace("new-table", "table").replace("load-table", "table")
        # init database
        db = db_init_function()
        query = gen_query_function(db, table_name)
        if LOG_LEVEL > 8: print(f"{'#'*10}\nStarting {sub_layer_name}\n{'#'*10}")
        is_loaded = layer_name.__contains__("load-table")
        # generate test cases
        testcase_set = create_tests(is_loaded, filler_tests)
        # transaction test
        phase = 0
        # build transaction pool
        insert_transactions = [Transaction() for _ in range(NUM_TRANSACTIONS)]
        transactions = [Transaction() for _ in range(NUM_TRANSACTIONS)]
        for i, test_case in enumerate(testcase_set):
            # add queries to transactions
            q = test_case.query
            if q == QueryType.INSERT:
                assert phase == 0 # must add insert queries first
                insert_transactions[i%NUM_TRANSACTIONS].add_query(query.insert, None, *test_case.kwargs["columns"])
            elif q == QueryType.UPDATE:
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.update, None, test_case.kwargs["primary_key"], *test_case.kwargs["columns"])
            elif q == QueryType.SELECT:
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.select, None, *test_case.kwargs)
            elif q == QueryType.SELECT_VERSION:
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.select_version, None, *test_case.kwargs)
            elif q == QueryType.SUM:
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.sum, None, *test_case.kwargs)
            elif q == QueryType.SUM_VERSION:
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.sum_version, None, *test_case.kwargs)
            else: # delete
                if phase == 0:
                    phase += 1
                transactions[i%NUM_TRANSACTIONS].add_query(query.delete, None, *test_case.kwargs)
        # run transactions
        # parallel transactions test
        if layer_name.__contains__("parallel"):
            # split transactions into transaction_workers
            transaction_workers = [TransactionWorker() for _ in range(NUM_THREADS)]
            # do inserts
            for i,t in enumerate(insert_transactions):
                transaction_workers[i%NUM_THREADS].add_transaction(t)
            try:
                # run insert transactions
                for i in range(NUM_THREADS):
                    transaction_workers[i].run()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Insert Transactions Worker Run :: Exception=={e}")
            try:
                # wait for workers to finish
                for i in range(NUM_THREADS):
                    transaction_workers[i].join()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Insert Transactions Worker Join :: Exception=={e}")

            # do all other queries
            for i,t in enumerate(transactions):
                transaction_workers[i%NUM_THREADS].add_transaction(t)
            try:
                # run transactions
                for i in range(NUM_THREADS):
                    transaction_workers[i].run()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Other Transactions Worker Run :: Exception=={e}")
            try:
                # wait for workers to finish
                for i in range(NUM_THREADS):
                    transaction_workers[i].join()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Other Transactions Worker Join :: Exception=={e}")
        # squential transaction test
        else:
            # run transactions squentially (won't work for small numbers of transactions)
            assert len(testcase_set) < NUM_TRANSACTIONS # actual numbers are a little different since inserts are done seperatly
            try:
                for t in insert_transactions:
                    t.run()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Insert Transactions :: Exception=={e}")
            try:
                for t in transactions:
                    t.run()
            except Exception as e:
                if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} Other Transactions :: Exception=={e}")

        # check results
        phase = 0
        insert_counter = {}
        other_counter = {}
        # build a set of results
        for i, test_case in enumerate(testcase_set):
            if test_case.query == QueryType.INSERT:
                assert phase == 0
                x = i % NUM_TRANSACTIONS
                results = insert_transactions[x].results
                if x in insert_counter.keys():
                    # already been to this transaction, access the next value
                    insert_counter[x] += 1
                else:
                    insert_counter[x] = 0
                # update results
                test_case.update_result(results[insert_counter[x]])
            else:
                if phase == 0:
                    phase += 1
                x = i % NUM_TRANSACTIONS
                results = transactions[x].results
                if x in other_counter.keys():
                    # already been to this transaction, access the next value
                    other_counter[x] += 1
                else:
                    other_counter[x] = 0
                # update results
                test_case.update_result(results[other_counter[x]])

        correctness_two = evaluate_correctness(testcase_set)
        if False in correctness_two:
            if LOG_LEVEL >= 0: print(f"!!!!! FAILED Test {sub_layer_name} !!!!!")
        else:
            if LOG_LEVEL >= 0: print(f"::::: PASSED Test {sub_layer_name} :::::")
        db.close()

    def init_database(self) -> Database:
        db = Database()
        db.open(DB_NAME)
        return db

    def noinit_database(self) -> Database:
        db = Database()
        return db

    def new_table(self, db:Database, name:str) -> Query:
        table = db.create_table(name, 5, 0)
        query = Query(table)
        return query

    def load_table(self, db:Database, name:str) -> Query:
        table = db.get_table(name)
        assert type(table) == Table
        query = Query(table)
        return query


if __name__ == "__main__":
    test_system = DatabaseLayer()
    test_system.run()
