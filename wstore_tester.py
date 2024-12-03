from lstore.db import Database
from lstore.query import Query
from lstore.table import Table
from lstore.transaction import Transaction
from pathlib import Path
from enum import Enum
from typing import Callable

db_name = "WstoreTesterDatabase"
answerkey_file = Path("wstore_tester_answerkey.json")
num_col = 5
LOG_LEVEL = 9

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
    def __init__(self, query:QueryType, answer, **kwargs):
        self.query = query
        self.kwargs = kwargs
        self.answer = answer
        self.__result = None
        self.result_updated = False

    def update_result(self, value):
        self.__result = value
        self.result_updated = True

    def evaluate(self) -> bool:
        if self.result_updated:
            try:
                k = " ".join([f"{x}, {y}" for x, y in self.kwargs.items()])
                if self.answer == self.__result:
                    if LOG_LEVEL > 3: print(f"PASSED:::: {self.query.name} {k} ::::")
                    return True
                else:
                    if LOG_LEVEL > 4: print(f"FAILED:::: {self.query.name} {k} ::::\t\tAnswer=={self.answer}\t\tResult=={self.__result}")
                    return False
            except Exception as e:
                k = " ".join([f"{x}, {y}" for x, y in self.kwargs.items()])
                if LOG_LEVEL > 6: print(f"#### EXCEPTION {e} in {self.query.name} {k} ####")
                return False
        else:
            if LOG_LEVEL > 5: print("!!!Attempted to evaluate result but result was not updated!!!")
            return False


def evaluate_correctness(test_cases:list[Test]) -> list[bool]:
    """
    evaluates the correctness of the given test_case list, assuming all test results have been updated.
    """
    correctness = [False]*len(test_cases)
    for i, test in enumerate(test_cases):
        correctness[i] = test.evaluate()
    return correctness


def prepend_sorted_tests(eval_test_cases:list[Test], prepend_test_cases:list[Test]) -> list[Test]:
    new_list = []
    end_list_one = False
    end_list_two = False
    i = 0
    j = 0
    while not end_list_one and not end_list_two:
        if i >= len(eval_test_cases):
            i = len(eval_test_cases) - 1
            end_list_one = True
        if j >= len(prepend_test_cases):
            j = len(prepend_test_cases) - 1
            end_list_two = True
        if end_list_one and end_list_two:
            pass
        else:
            if eval_test_cases[i].query == prepend_test_cases[j].query:
                # lists are in sync insert prepend list items
                new_list.append(prepend_test_cases[j])
                j += 1
            elif eval_test_cases[i].query.value < prepend_test_cases[j].query.value:
                # finished adding prepend list for this section
                new_list.append(eval_test_cases[i])
                i += 1
            else:
                # eval value > prepend value
                # prepend list is behind eval list, same as lists in sync
                new_list.append(prepend_test_cases[j])
                j += 1
    return new_list


def create_tests(is_loaded=False) -> list[Test]:
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
        for record in base_records:
            tests.append(Test(QueryType.INSERT, True, columns=record))
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
        for record in bad_base_records:
            tests.append(Test(QueryType.INSERT, False, columns=record))
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
            tests.append(Test(QueryType.UPDATE, True, primary_key=920000+i, columns=tail_records[i]))
    # test bad updates
    bad_tail_records = [
            [],
            [None]*5,
            [920006, None, 555, None, None], # update to existing primary key
            [920002, 999, 666, 333, 111] # this is valid but we will delete it later, in an invalid way
            ]
    if not is_loaded:
        tests.append(Test(QueryType.UPDATE, False, primary_key=920004, columns=bad_tail_records[0]))
        tests.append(Test(QueryType.UPDATE, False, primary_key=920005, columns=bad_tail_records[1]))
        tests.append(Test(QueryType.UPDATE, False, primary_key=920001, columns=bad_tail_records[2]))
        tests.append(Test(QueryType.UPDATE, True, primary_key=920009, columns=bad_tail_records[3]))
    # test select
    tests.append(Test(QueryType.SELECT, add_lists(base_records[0], tail_records[0]), search_key=920000, search_key_index=0, projected_columns_index=[1]*5))
    # test bad select
    tests.append(Test(QueryType.SELECT, [], search_key=999999, search_key_index=0, projected_columns_index=[1]*5))
    tests.append(Test(QueryType.SELECT, [], search_key=920000, search_key_index=9, projected_columns_index=[1]*5))
    tests.append(Test(QueryType.SELECT, [], search_key=920000, search_key_index=0, projected_columns_index=[1]*7))
    # test select_version
    tests.append(Test(QueryType.SELECT_VERSION, add_lists(base_records[0], tail_records[0]), search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=0))
    tests.append(Test(QueryType.SELECT_VERSION, base_records[0], search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=-1))
    # test bad select_version
    tests.append(Test(QueryType.SELECT_VERSION, base_records[0], search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=-2))
    tests.append(Test(QueryType.SELECT_VERSION, base_records[0], search_key=920000, search_key_index=0, projected_columns_index=[1]*5, relative_version=-3))
    # test sum
    tests.append(Test(QueryType.SUM, 96, start_range=920006, end_range=920008, aggregate_column_index=3))
    # test bad sum
    tests.append(Test(QueryType.SUM, 0, start_range=999989, end_range=999999, aggregate_column_index=3))
    # test sum_version
    tests.append(Test(QueryType.SUM_VERSION, 15, start_range=920006, end_range=920008, aggregate_column_index=3, relative_version=-1))
    # test bad sum_version
    tests.append(Test(QueryType.SUM_VERSION, 15, start_range=920006, end_range=920008, aggregate_column_index=3, relative_version=-2))
    tests.append(Test(QueryType.SUM_VERSION, 15, start_range=920006, end_range=920008, aggregate_column_index=3, relative_version=-4))
    tests.append(Test(QueryType.SUM_VERSION, 0, start_range=999989, end_range=999999, aggregate_column_index=3, relative_version=-1))

    # test delete
    if not is_loaded:
        tests.append(Test(QueryType.DELETE, True, primary_key=920000))
        # test bad deletes
        # not allowed since this will uncover primary key 920002, and 920009 was updated to 920002
        tests.append(Test(QueryType.DELETE, False, primary_key=120000))
        # not a primary key
        tests.append(Test(QueryType.DELETE, False, primary_key=0))
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
        self.database_init_layer("single-page")
        # multi-page test
        # self.database_init_layer("multi-page", test_cases)

    def database_init_layer(self, layer_name:str):
        # init database test
        db = self.init_database()
        self.load_table_layer(f"{layer_name}|init-database", db)
        # non-init database test
        db = self.noinit_database()
        self.load_table_layer(f"{layer_name}|non-init-database", db)

    def load_table_layer(self, layer_name:str, db:Database):
        # new table test
        self.concurrency_layer(f"{layer_name}|new-table", self.new_table, db)
        # load table test
        self.concurrency_layer(f"{layer_name}|load-table", self.load_table, db)

    def concurrency_layer(self, layer_name:str, gen_query_function:Callable[[Database, str], Query], db:Database):
        # serial test
        self.transaction_layer(f"{layer_name}|serial", gen_query_function, db)
        # parallel test (transaction workers)
        # self.transaction_layer(f"{layer_name}|parallel", query, test_cases)
        # self.transaction_layer()

    def transaction_layer(self, layer_name:str, gen_query_function:Callable[[Database, str], Query], db:Database):
        # ### serial tests only ###
        if not layer_name.__contains__("parallel"):
            sub_layer_name = f"{layer_name}|queries"
            table_name = sub_layer_name.replace("new-table", "table").replace("load-table", "table")
            query_one = gen_query_function(db, table_name)
            if LOG_LEVEL > 8: print(f"{'#'*10}\nStarting {sub_layer_name}\n{'#'*10}")
            is_loaded = layer_name.__contains__("load-table")
            testcase_set_one = create_tests(is_loaded)
            # non-transaction test (just queries)
            for test_case in testcase_set_one:
                # run queries and update results
                q = test_case.query
                if q == QueryType.INSERT:
                    try:
                        result = query_one.insert(*test_case.kwargs["columns"])
                        if result is not False:
                            test_case.update_result(True)
                        else:
                            test_case.update_result(False)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} INSERT :: Exception=={e}")
                elif q == QueryType.UPDATE:
                    try:
                        result = query_one.update(test_case.kwargs["primary_key"], *test_case.kwargs["columns"])
                        if result is not False:
                            test_case.update_result(True)
                        else:
                            test_case.update_result(False)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} UPDATE :: Exception=={e}")
                elif q == QueryType.SELECT:
                    try:
                        result = query_one.select(**test_case.kwargs)
                        if len(result) > 0:
                            r = result[0].columns
                        else:
                            r = result
                        test_case.update_result(r)
                    except Exception as e:
                        if LOG_LEVEL > 7: print(f"FAILED during {sub_layer_name} SELECT :: Exception=={e}")
                elif q == QueryType.SELECT_VERSION:
                    try:
                        result = query_one.select_version(**test_case.kwargs)
                        if len(result) > 0:
                            r = result[0].columns
                        else:
                            r = result
                        test_case.update_result(r)
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

        # ### parallel and serial tests ###
        # transaction test
        # sub_layer_name = f"{layer_name}|transaction"
        # table_name = sub_layer_name.replace("new-table", "table").replace("load-table", "table")
        # query_two = gen_query_function(db, table_name)
        # if LOG_LEVEL > 8: print(f"{'#'*10}\nStarting {sub_layer_name}\n{'#'*10}")
        # testcase_set_two = create_tests()
        # db.close()

    def init_database(self) -> Database:
        db = Database()
        db.open(db_name)
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
