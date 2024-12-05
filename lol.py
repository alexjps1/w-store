from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []



# insert, update, delete
print("Insert, update, delete")
query.insert(24, 93, 0, 0, 0)
query.update(24, 34, 1, 1, 1, 1)
query.delete(24)
print("Done")
query.delete(34)

"""
# insert then delete
print("Insert and delete immediately")
query.insert(24, 93, 0, 0, 0)
query.delete(24)
print("Done")
"""

exit()
