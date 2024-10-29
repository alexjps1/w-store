from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
import matplotlib.pyplot as plt


class Graph_Gen:
    def __init__(self):
        self.primary_key_base = 906659671
        self.title_font = { 'family':"serif", "color":"red", 'size':20}
        self.label_font = { 'family':"serif", 'color':'blue', 'size':15}

    def main(self):
        db = Database()
        # self.run_tail_record_test(db, base_inserts=100, num_queries=1000, domain=100)
        # self.run_page_test(db, inserts=1000, domain=100)

    def speed_plots(self):
        # run graph analysis
        sum_size = 150
    
        n_records = 5000
        n_updates = 5000
        n_selects = 5000
        n_ranges = 500
        n_deletes = 1000
        # record: delta t, for insert, delete, select, update, sum
        # cumulative/non, B+, Hash, Dumb
        delta_t = {
                0:{
                   0: [0.0]*n_records,     # insert
                   1: [0.0]*n_updates,  # update
                   2: [0.0]*n_selects,  # select
                   4: [0.0]*n_ranges,  # sum large
                   5: [0.0]*n_deletes   # delete
                }}
    
        db = Database()
        table_settings = {
                "cumulative (hashmap)": [True, 4096, 4, False, False, 4],
                "non-cumulative (hashmap)": [False, 4096, 4, False, False, 4],
                "cumulative (b+ tree)": [True, 4096, 4, True, False, 4],
                "non-cumulative (b+ tree)": [False, 4096, 4, True, False, 4],
                # "b+ tree max degree 4": [True, 4096, 4, True, False, 4],
                "b+ tree max degree 8": [True, 4096, 4, True, False, 8],
                "b+ tree max degree 16": [True, 4096, 4, True, False, 16],
                "b+ tree max degree 32": [True, 4096, 4, True, False, 32],
                "b+ tree max degree 64": [True, 4096, 4, True, False, 64],
                }

        for j, setting in enumerate(table_settings):
            delta_t[j] = self.run_operations_experiment(n_records, n_updates, n_selects, n_ranges, n_deletes, sum_size,
                                                        db,
                                                        table_settings[setting])
    
        # NOTE for: cumulative/non, B+, Hash, Dumb
        # record: delta t, for insert, delete, select, update, sum
        # x record: page size, record size, # versions, degree, 
        """
        ######### cumulative vs non-cumulative tail records on B+ and Hash #########
        """
        labels = list(table_settings.keys())
        fig, ax = plt.subplots()
        # inserts
        ax.plot(delta_t[0][0], label='inserts - {}'.format(labels[0]))
        ax.plot(delta_t[1][0], label='inserts - {}'.format(labels[1]))
        ax.plot(delta_t[2][0], label='inserts - {}'.format(labels[2]))
        ax.plot(delta_t[3][0], label='inserts - {}'.format(labels[3]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Insert Performance", fontdict=self.title_font)
        plt.show()
    
        fig, ax = plt.subplots()
        # updates
        ax.plot(delta_t[0][1], label='updates - {}'.format(labels[0]))
        ax.plot(delta_t[1][1], label='updates - {}'.format(labels[1]))
        ax.plot(delta_t[2][1], label='updates - {}'.format(labels[2]))
        ax.plot(delta_t[3][1], label='updates - {}'.format(labels[3]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Update Performance", fontdict=self.title_font)
        plt.show()
    
        fig, ax = plt.subplots()
        # selects
        ax.plot(delta_t[0][2], label='selects - {}'.format(labels[0]))
        ax.plot(delta_t[1][2], label='selects - {}'.format(labels[1]))
        ax.plot(delta_t[2][2], label='selects - {}'.format(labels[2]))
        ax.plot(delta_t[3][2], label='selects - {}'.format(labels[3]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Point Query Performance", fontdict=self.title_font)
        plt.show()
    
        fig, ax = plt.subplots()
        # sums
        ax.plot(delta_t[0][4], label='sum range({}) - {}'.format(sum_size, labels[0]))
        ax.plot(delta_t[1][4], label='sum range({}) - {}'.format(sum_size, labels[1]))
        ax.plot(delta_t[2][4], label='sum range({}) - {}'.format(sum_size, labels[2]))
        ax.plot(delta_t[3][4], label='sum range({}) - {}'.format(sum_size, labels[3]))
    
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Range Query Performance", fontdict=self.title_font)
        plt.show()
    
        """
        ######### Effects of Max Degree on B+ Tree Performance #########
        """
        labels = list(table_settings.keys())
        fig, ax = plt.subplots()
        # inserts
        ax.plot(delta_t[2][0], label='inserts - b+ tree max degree 4')
        ax.plot(delta_t[4][0], label='inserts - {}'.format(labels[4]))
        ax.plot(delta_t[5][0], label='inserts - {}'.format(labels[5]))
        ax.plot(delta_t[6][0], label='inserts - {}'.format(labels[6]))
        ax.plot(delta_t[7][0], label='inserts - {}'.format(labels[7]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Insert Performance", fontdict=self.title_font)
        plt.show()
    
        fig, ax = plt.subplots()
        # updates
        ax.plot(delta_t[2][1], label='updates - b+ tree max degree 4')
        ax.plot(delta_t[4][1], label='updates - {}'.format(labels[4]))
        ax.plot(delta_t[5][1], label='updates - {}'.format(labels[5]))
        ax.plot(delta_t[6][1], label='updates - {}'.format(labels[6]))
        ax.plot(delta_t[7][1], label='updates - {}'.format(labels[7]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Update Performance", fontdict=self.title_font)
        plt.show()
    
        fig, ax = plt.subplots()
        # selects
        ax.plot(delta_t[2][2], label='selects - b+ tree max degree 4')
        ax.plot(delta_t[4][2], label='selects - {}'.format(labels[4]))
        ax.plot(delta_t[5][2], label='selects - {}'.format(labels[5]))
        ax.plot(delta_t[6][2], label='selects - {}'.format(labels[6]))
        ax.plot(delta_t[7][2], label='selects - {}'.format(labels[7]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Point Query Performance", fontdict=self.title_font)
        plt.show()
        fig, ax = plt.subplots()
        # B+ Tree Degree
        ax.plot(delta_t[2][4], label='sum range({}) - b+ tree max degree 4'.format(sum_size))
        ax.plot(delta_t[4][4], label='sum range({}) - {}'.format(sum_size, labels[4]))
        ax.plot(delta_t[5][4], label='sum range({}) - {}'.format(sum_size, labels[5]))
        ax.plot(delta_t[6][4], label='sum range({}) - {}'.format(sum_size, labels[6]))
        ax.plot(delta_t[7][4], label='sum range({}) - {}'.format(sum_size, labels[7]))
        # labels and settings
        ax.set_xlabel('operations (#)', fontdict=self.label_font)
        ax.set_ylabel('time (s)', fontdict=self.label_font)
        ax.legend()
        ax.set_title("Range Query Performance", fontdict=self.title_font)
        plt.show()
    
        # Variations of B+ Tree Max Degree
        # x=max degree, y=t/N operations
        # - insert
        # - delete
        # - select
        # - update
        # - sum
        
        # B+ Tree vs Hashmap vs DumbIndex on speed
        # x=N operations, y=t
        # - insert
        # - delete
        # - select
        # - update
        # - sum
        
        # B+ Tree
        # - O(log[n/2] N) vs real world
        # x=operations, y=t
        # - insert
        # - delete
        # - select
        # - update
        # - sum
        
        # Hashmap
        # O(N) vs real world
        # x=operations, y=t
        # - insert
        # - delete
        # - select
        # - update
        # - sum
    
        # DumbIndex (lookup)
        # O(N**2) vs real world
        # x=operations, y=t
        # - insert
        # - delete
        # - select
        # - update
        # - sum
        pass

    def run_page_test(self, db:Database, inserts:int, domain:int):
        """
        # Variations on page size and record size on speed
        # x=page size, y=t/N operations
        # x=record size, y=t/N operations
        # - insert
        # - update
        # - select
        # - sum
        """
        # data = {}
        opp_times = {0:[], 1:[]}
        for i in range(domain):
            _, t = self.run_operations_experiment(num_inserts=inserts,
                                              num_updates=inserts,
                                              num_selects=inserts,
                                              num_ranges=inserts,
                                              num_deletes=0,
                                              sum_size=0,
                                              db=db,
                                              settings=[False, 4096*(i+1), 4, False, False, 4],
                                              query_skip_map=[False, False, False, False, True],
                                              update_all=False)
            opp_times[0].append(t[0] + t[1] + t[2])
        for i in range(domain):
            _, t = self.run_operations_experiment(num_inserts=inserts,
                                              num_updates=inserts,
                                              num_selects=inserts,
                                              num_ranges=inserts,
                                              num_deletes=0,
                                              sum_size=0,
                                              db=db,
                                              settings=[False, 4096, 4*(i+1), False, False, 4],
                                              query_skip_map=[False, False, False, False, True],
                                              update_all=False)
            opp_times[1].append(t[0] + t[1] + t[2])

        page_x = [4096*(i+1) for i in range(len(opp_times[0]))]
        record_x = [4*(i+1) for i in range(len(opp_times[1]))]

        fig, ax = plt.subplots()
        ax.plot(record_x, opp_times[1], label='{}'.format("record size"))
        # labels and settings
        ax.set_xlabel('record size (bytes)', fontdict=self.label_font)
        ax.set_ylabel('time (s)/{} operations'.format(inserts*4), fontdict=self.label_font)
        ax.legend()
        ax.set_title("Total Performance vs Size of Records (Fixed Page size 4096B)", fontdict=self.title_font)
        plt.show()
        fig, ax = plt.subplots()
        ax.plot(page_x, opp_times[0], label='{}'.format("page size"))
        # labels and settings
        ax.set_xlabel('page size (bytes)', fontdict=self.label_font)
        ax.set_ylabel('time (s)/{} operations'.format(inserts*4), fontdict=self.label_font)
        ax.legend()
        ax.set_title("Total Performance vs Size of Pages (Fixed Record Size 4B)", fontdict=self.title_font)
        plt.show()

    def run_tail_record_test(self, db:Database, base_inserts:int, num_queries:int, domain:int):
        """
        # Non-cumulative vs cumulative tail records
        # multi table same settings, variable samples
        # - x=# of versions, y=t/N operations
        # - select
        # - # versions
        """
        # data = {}
        select_times = {0:[],1:[]}
        #non-cumulative
        for i in range(domain):
            _, t = self.run_operations_experiment(num_inserts=base_inserts,
                                              num_updates=base_inserts*i,
                                              num_selects=num_queries,
                                              num_ranges=0,
                                              num_deletes=0,
                                              sum_size=0,
                                              db=db,
                                              settings=[False, 4096, 4, False, False, 4],
                                              query_skip_map=[False, False, False, True, True],
                                              update_all=True)
            select_times[0].append(t[2])
        # cumulative
        for i in range(domain):
            _, t = self.run_operations_experiment(num_inserts=base_inserts,
                                              num_updates=base_inserts*i,
                                              num_selects=num_queries,
                                              num_ranges=0,
                                              num_deletes=0,
                                              sum_size=0,
                                              db=db,
                                              settings=[True, 4096, 4, False, False, 4],
                                              query_skip_map=[False, False, False, True, True],
                                              update_all=True)
            select_times[1].append(t[2])

        fig, ax = plt.subplots()
        ax.plot(select_times[0], label='{}'.format("non-cumulative tails"))
        ax.plot(select_times[1], label='{}'.format("cumulative tails"))
        # labels and settings
        ax.set_xlabel('# of tail records per base', fontdict=self.label_font)
        ax.set_ylabel('time (s)/{} queries'.format(num_queries), fontdict=self.label_font)
        ax.legend()
        ax.set_title("Point Query Performance vs Number of Tail Records", fontdict=self.title_font)
        plt.show()

    
    def run_operations_experiment(self,
                                  num_inserts:int,
                                  num_updates:int,
                                  num_selects:int,
                                  num_ranges:int,
                                  num_deletes:int,
                                  sum_size:int,
                                  db:Database,
                                  settings:list,
                                  query_skip_map:list[bool]=[False, False, False, False, True],
                                  update_all:bool=False):
        delta_t = {
               0: [0.0]*num_inserts,     # insert
               1: [0.0]*num_updates,  # update
               2: [0.0]*num_selects,  # select
               4: [0.0]*num_ranges,  # sum large
               5: [0.0]*num_deletes   # delete
                }
        t = [0.0]*5
        grades_table = db.create_table('Grades', 5, 0,
                                       cumulative_tails=settings[0],   # True = cumulative tail records, False = non-cumulative tail records
                                       page_size=settings[1],          # total size of pages in bytes
                                       record_size=settings[2],           # size of partial records in bytes
                                       use_bplus=settings[3],         # True = B+ tree index, False = hashmap index
                                       use_dumbindex=settings[4],     # override smart index with dumb index
                                       bplus_degree=settings[5]          # degree of b+ tree
                                       )
    
        query = Query(grades_table)
        keys = [0]*num_inserts
    
        update_cols = [
            [None, None, None, None, None],
            [None, randrange(0, 100), None, None, None],
            [None, None, randrange(0, 100), None, None],
            [None, None, None, randrange(0, 100), None],
            [None, None, None, None, randrange(0, 100)],
        ]
        if not query_skip_map[0]:
            # insert records
            insert_time_0 = process_time()
            for i in range(0, num_inserts):
                query.insert(self.primary_key_base + i, 93, 0, 0, 0)
                t1 = process_time()
                delta_t[0][i] = t1 - insert_time_0
                keys[i] = self.primary_key_base + i
            insert_time_1 = process_time()
            t[0] = insert_time_1 - insert_time_0
            print("Inserting {} records took:  \t\t\t".format(num_inserts), t[0])
    
        if not query_skip_map[1]:
            # update records
            if update_all:
                # no random updates, update records in sequence
                update_time_0 = process_time()
                for i in range(0, num_updates):
                    # t0 = process_time()
                    query.update(keys[i%num_inserts], *(choice(update_cols)))
                    t1 = process_time()
                    delta_t[1][i] = t1 - update_time_0
                update_time_1 = process_time()
            else:
                update_time_0 = process_time()
                for i in range(0, num_updates):
                    # t0 = process_time()
                    query.update(choice(keys), *(choice(update_cols)))
                    t1 = process_time()
                    delta_t[1][i] = t1 - update_time_0
                update_time_1 = process_time()
            t[1] = update_time_1 - update_time_0
            print("Updating {} records took:  \t\t\t".format(num_updates), t[1])
    
        if not query_skip_map[2]:
            # select records
            select_time_0 = process_time()
            for i in range(0, num_selects):
                query.select(choice(keys),0 , [1, 1, 1, 1, 1])
                t1 = process_time()
                delta_t[2][i] = t1 - select_time_0
            select_time_1 = process_time()
            t[2] = select_time_1 - select_time_0
            print("Selecting {} records took:  \t\t\t".format(num_selects), t[2])
    
        if not query_skip_map[3]:
            # sum records large range
            agg_time_0 = process_time()
            for i in range(0, num_ranges):
                start_value = self.primary_key_base + randrange(0, num_inserts - sum_size)
                end_value = start_value + sum_size
    
                result = query.sum(start_value, end_value - 1, randrange(0, 5))
    
                t1 = process_time()
                delta_t[4][i] = t1 - agg_time_0
            agg_time_1 = process_time()
            t[3] = agg_time_1 - agg_time_0
            print("Sum {} of {} record batch took:\t".format(num_ranges, sum_size), t[3])
    
        if not query_skip_map[4]:
            # delete
            assert False
    
        # clean up for next run
        db.drop_table("Grades") 
        return delta_t, t


if __name__ == "__main__":
    app = Graph_Gen()
    app.main()
