from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
import matplotlib.pyplot as plt


if __name__ == "__main__":
    # run graph analysis
    sum_size_s = 100
    sum_size_l = 5000

    number_of_records = 5000000
    number_of_updates = 5000000
    number_of_selects = 5000000
    number_of_ranges_s = 10000
    number_of_ranges_l = 10000
    number_of_deletes = 1000
    # record: delta t, for insert, delete, select, update, sum
    # cumulative/non, B+, Hash, Dumb
    delta_t = {
            0:{
               0: [0.0]*number_of_records,     # insert
               1: [0.0]*number_of_updates,  # update
               2: [0.0]*number_of_selects,  # select
               3: [0.0]*number_of_ranges_s,  # sum small
               4: [0.0]*number_of_ranges_l,  # sum large
               5: [0.0]*number_of_deletes   # delete
            }}
    primary_key_base = 906659671

    db = Database()
    table_settings = [
            [True, 4096, 4, False, False, 4],
            [False, 4096, 4, False, False, 4]
            ]
    for sx, settings in enumerate(table_settings):
        delta_t[sx] = {
               0: [0.0]*number_of_records,     # insert
               1: [0.0]*number_of_updates,  # update
               2: [0.0]*number_of_selects,  # select
               3: [0.0]*number_of_ranges_s,  # sum small
               4: [0.0]*number_of_ranges_l,  # sum large
               5: [0.0]*number_of_deletes   # delete
                }
        grades_table = db.create_table('Grades', 5, 0,
                                       cumulative_tails=settings[0],   # True = cumulative tail records, False = non-cumulative tail records
                                       page_size=settings[1],          # total size of pages in bytes
                                       record_size=settings[2],           # size of partial records in bytes
                                       use_bplus=settings[3],         # True = B+ tree index, False = hashmap index
                                       use_dumbindex=settings[4],     # override smart index with dumb index
                                       bplus_degree=settings[5]          # degree of b+ tree
                                       )

        query = Query(grades_table)
        keys = [0]*number_of_records

        update_cols = [
            [None, None, None, None, None],
            [None, randrange(0, 100), None, None, None],
            [None, None, randrange(0, 100), None, None],
            [None, None, None, randrange(0, 100), None],
            [None, None, None, None, randrange(0, 100)],
        ]
        # insert records
        insert_time_0 = process_time()
        for i in range(0, number_of_records):
            query.insert(primary_key_base + i, 93, 0, 0, 0)
            t1 = process_time()
            delta_t[sx][0][i] = t1 - insert_time_0
            keys[i] = primary_key_base + i
        insert_time_1 = process_time()
        
        print("Inserting {} records took:  \t\t\t".format(number_of_records), insert_time_1 - insert_time_0)

        # update records
        update_time_0 = process_time()
        for i in range(0, number_of_updates):
            # t0 = process_time()
            query.update(choice(keys), *(choice(update_cols)))
            t1 = process_time()
            delta_t[sx][1][i] = t1 - update_time_0
        update_time_1 = process_time()
        print("Updating {} records took:  \t\t\t".format(number_of_updates), update_time_1 - update_time_0)

        # select records
        select_time_0 = process_time()
        for i in range(0, number_of_selects):
            query.select(choice(keys),0 , [1, 1, 1, 1, 1])
            t1 = process_time()
            delta_t[sx][2][i] = t1 - select_time_0
        select_time_1 = process_time()
        print("Selecting {} records took:  \t\t\t".format(number_of_selects), select_time_1 - select_time_0)

        # sum records small range
        agg_time_0 = process_time()
        for i in range(0, number_of_ranges_s):
            start_value = primary_key_base + randrange(0, number_of_records - sum_size_s)
            end_value = start_value + sum_size_s

            result = query.sum(start_value, end_value - 1, randrange(0, 5))

            t1 = process_time()
            delta_t[sx][3][i] = t1 - agg_time_0
        agg_time_1 = process_time()
        print("Sum {} of {} record batch took:\t".format(number_of_ranges_s, sum_size_s), agg_time_1 - agg_time_0)

        # sum records large range
        agg_time_0 = process_time()
        for i in range(0, number_of_ranges_l):
            start_value = primary_key_base + randrange(0, number_of_records - sum_size_l)
            end_value = start_value + sum_size_l

            result = query.sum(start_value, end_value - 1, randrange(0, 5))

            t1 = process_time()
            delta_t[sx][4][i] = t1 - agg_time_0
        agg_time_1 = process_time()
        print("Sum {} of {} record batch took:\t".format(number_of_ranges_l, sum_size_l), agg_time_1 - agg_time_0)
    
    # delete

    # NOTE for: cumulative/non, B+, Hash, Dumb
    # record: delta t, for insert, delete, select, update, sum
    # x record: page size, record size, # versions, degree, 
    fig, ax = plt.subplots()
    ax.plot(delta_t[0][0], label='cumulative')
    ax.plot(delta_t[1][0], label='non-cumulative')
    ax.set_xlabel('insertions (#)')
    ax.set_ylabel('time (s)')
    ax.legend()
    ax.set_title("Cumulative vs Non-Cumulative Tail Records with Hashmaps Insert")
    plt.show()

    # updates
    fig, ax = plt.subplots()
    ax.plot(delta_t[0][1], label='cumulative')
    ax.plot(delta_t[1][1], label='non-cumulative')
    ax.set_xlabel('updates (#)')
    ax.set_ylabel('time (s)')
    ax.legend()
    ax.set_title("Cumulative vs Non-Cumulative Tail Records with Hashmaps Update")
    plt.show()

    # select
    fig, ax = plt.subplots()
    ax.plot(delta_t[0][2], label='cumulative')
    ax.plot(delta_t[1][2], label='non-cumulative')
    ax.set_xlabel('selections (#)')
    ax.set_ylabel('time (s)')
    ax.legend()
    ax.set_title("Cumulative vs Non-Cumulative Tail Records with Hashmaps Select")
    plt.show()

    # ranges
    fig, ax = plt.subplots()
    ax.plot(delta_t[0][3], label='small range cumulative')
    ax.plot(delta_t[1][3], label='small range non-cumulative')
    ax.plot(delta_t[0][4], label='large range cumulative')
    ax.plot(delta_t[1][4], label='large range non-cumulative')
    ax.set_xlabel('ranges (#)')
    ax.set_ylabel('time (s)')
    ax.legend()
    ax.set_title("Cumulative vs Non-Cumulative Tail Records with Hashmaps Sum")
    plt.show()

    # Variations on page size and record size on speed
    # x=page size, y=t/N operations
    # x=record size, y=t/N operations
    # - insert
    # - update
    # - select
    # - sum
    # - - large range & small range
    
    # Non-cumulative vs cumulative tail records
    # - x=# of versions, y=t/N operations
    # - select
    # - # versions
    
    # Variations of B+ Tree Max Degree
    # x=max degree, y=t/N operations
    # - insert
    # - delete
    # - select
    # - update
    # - sum
    # - - large range & small range
    
    # B+ Tree vs Hashmap vs DumbIndex on speed
    # x=N operations, y=t
    # - insert
    # - delete
    # - select
    # - update
    # - sum
    # - - large range & small range
    
    # B+ Tree
    # - O(log[n/2] N) vs real world
    # x=operations, y=t
    # - insert
    # - delete
    # - select
    # - update
    # - sum
    # - - large range & small range
    
    # Hashmap
    # O(N) vs real world
    # x=operations, y=t
    # - insert
    # - delete
    # - select
    # - update
    # - sum
    # - - large range & small range

    # DumbIndex (lookup)
    # O(N**2) vs real world
    # x=operations, y=t
    # - insert
    # - delete
    # - select
    # - update
    # - sum
    # - - large range & small range
    pass
