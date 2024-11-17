from lstore.db import Database
from lstore.query import Query

db_name = "Project2Test"
num_col = 5
num_records = 800
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

def version_tester(name:str):
    """
    Runs insert, update, and select_version and compares results
    """
    # Test new database
    db = Database()
    db.open(db_name)
    table = db.create_table(name, num_col, 0)
    query = Query(table)

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
            m = i%(num_col - 1) + 1
            l[m] = i*n + 8
            updates.append(l)
        tail_records.append(updates)

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

if __name__ == "__main__":
    version_tester("VersionTable")
