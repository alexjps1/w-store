from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from random import shuffle

db_name = "Project2Test"
num_col = 5
num_records = 8
updates_per_record = 6


def add_lists(l1, l2):
    assert len(l1) == len(l2)
    return [l1[i] if l2[i] is None else l2[i] for i in range(len(l1))]

def test_select_version(primary_keys, base_records, tail_records, query:Query):
    # test versions
    # print("-> running select_version")
    for i, key in enumerate(primary_keys):
        base = base_records[i]
        answers = [base]
        answer = base
        for x in range(updates_per_record):
            tail = tail_records[i][x]
            # build expected solution
            answer = add_lists(answer, tail)
            answers.append(answer)
        # check answers
        errors = False
        for x in range(updates_per_record):
            records = query.select_version(key, 0, [1]*num_col, -x)
            tail = tail_records[i][x]
            if records[0].columns != answers[updates_per_record - x]:
                print(f"!!!! Select Error version::{-x}\t answer::{answers[updates_per_record - x]},\t returned::{records[0].columns}!!!!")
                errors = True
        # break if errors were found
        assert not errors
    print("#### select_version PASSED ####")
    
def build_records(update_column_selector=lambda x: x%(num_col - 1) + 1) -> tuple[list[int], list[list[int]], list[list[list[int|None]]]]:
    # Build solutions
    primary_keys = [9200000 + i for i in range(num_records)]
    base_records = []
    for n, key in enumerate(primary_keys):
        l = []
        for i in range(num_col):
            if i == 0:
                l.append(key)
            else:
                l.append(n*i)
        base_records.append(l)

    tail_records = []
    for n, key in enumerate(primary_keys):
        updates = []
        for i in range(updates_per_record):
            l = [None]*num_col
            m = update_column_selector(i)
            l[m] = i + n + m + 8
            updates.append(l)
        tail_records.append(updates)
    # shuffle(primary_keys)
    # shuffle(base_records)
    # shuffle(tail_records)
    return primary_keys, base_records, tail_records

def insert_into_table(query:Query, primary_keys:list[int], base_records:list[list[int]], tail_records:list[list[list[int|None]]]) -> None:
    # print(f"BASE RECORDS::\n{base_records}\nTAIL RECORDS::\n{tail_records}")
    # perform queries
    print("starting insert")
    n = 0
    for r in base_records:
        query.insert(*r)
        n += 1
    print(f"Inserted {n} Records")
    print("starting updates")
    n = 0
    for i, key in enumerate(primary_keys):
        for u in tail_records[i]:
            query.update(key, *u)
            n += 1
    print(f"Appended {n} Tail Records")

def transaction_into_table(query:Query, table:"Table", primary_keys:list[int], base_records:list[list[int]], tail_records:list[list[list[int|None]]]) -> None:
    # print(f"BASE RECORDS::\n{base_records}\nTAIL RECORDS::\n{tail_records}")
    # perform queries
    transaction = Transaction()
    print("building transaction")
    n = 0
    for r in base_records:
        transaction.add_query(query.insert, table, *r)
        # query.insert(*r)
        n += 1
    n = 0
    for i, key in enumerate(primary_keys):
        for u in tail_records[i]:
            transaction.add_query(query.update, table, key, *u)
            # query.update(key, *u)
            n += 1
    print("running transaction")
    r = transaction.run()
    print("transaction successful" if r else "transaction failed")

def version_tester(name:str):
    """
    Runs insert, update, and select_version and compares results
    """
    # Test new database
    db = Database()
    db.open(db_name)
    table = db.create_table(name, num_col, 0)
    query = Query(table)

    primary_keys, base_records, tail_records = build_records()
    insert_into_table(query, primary_keys, base_records, tail_records)

    print("\n-- Testing select_version on NEW Table --\n")
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    print("closing database")
    db.close()

    # test loading a existing table
    print("\n-- Testing select_version on LOADED Table --\n")
    db = Database()
    db.open(db_name)
    table = db.get_table(name)
    query = Query(table)
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    print("closing database")
    db.close()

def uneven_updates_tester(name:str):
    """
    Runs insert, update, and select_version and compares results
    """
    # Test new database
    db = Database()
    db.open(db_name)
    table = db.create_table(name, num_col, 0)
    query = Query(table)

    primary_keys, base_records, tail_records = build_records(update_column_selector=lambda x:1)
    insert_into_table(query, primary_keys, base_records, tail_records)

    print("\n-- Testing select_version on NEW Table with Uneven Updates --\n")
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    print("closing database")
    db.close()

    # test loading a existing table
    print("\n-- Testing select_version on LOADED Table with Uneven Updates --\n")
    db = Database()
    db.open(db_name)
    table = db.get_table(name)
    query = Query(table)
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    print("closing database")
    db.close()

def test_transaction(name:str):
    """
    """
    # Test new database
    db = Database()
    db.open(db_name)
    table = db.create_table(name, num_col, 0)
    query = Query(table)

    primary_keys, base_records, tail_records = build_records(update_column_selector=lambda x:1)
    transaction_into_table(query, table, primary_keys, base_records, tail_records)

    print("\n-- Testing transaction on NEW Table --\n")
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    # print("closing database")
    db.close()

    # test loading a existing table
    print("\n-- Testing transaction on LOADED Table --\n")
    db = Database()
    db.open(db_name)
    table = db.get_table(name)
    query = Query(table)
    # compare solutions to query results
    test_select_version(primary_keys, base_records, tail_records, query)
    # print("closing database")
    db.close()

if __name__ == "__main__":
    version_tester("VersionTable")
    uneven_updates_tester("UnevenTable")
    test_transaction("TransactionTable")
