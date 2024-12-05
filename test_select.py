from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# range 7-11
query.insert(43342,  1,  5,   0,  7  )
query.insert(45345,  1,  32,  0,  8  )
query.insert(47349,  1,  32,  0,  10 )
query.insert(41360,  1,  32,  4,  12  )
query.insert(91369,  1,  5,   4,  0  )
query.insert(41393,  1,  5,   4,  9  )
query.insert(41420,  1,  32,  0,  6  )

results = query.sum(7, 11, 4)
for record in results:
    print(record.columns)
"""
primary_keys = [record.columns[4] for record in results]
print("selected primary keys:", primary_keys)
assert set(primary_keys) == set([32])
print("passed")
"""
